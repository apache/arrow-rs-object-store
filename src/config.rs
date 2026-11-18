// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
use std::fmt::{Debug, Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

use http::header::HeaderValue;
use jiff::fmt::friendly::SpanPrinter;

use crate::{Error, Result};

/// Provides deferred parsing of a value
///
/// This allows builders to defer fallibility to build
#[derive(Debug, Clone)]
pub(crate) enum ConfigValue<T> {
    Parsed(T),
    Deferred(String),
}

impl<T: Display> Display for ConfigValue<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Parsed(v) => write!(f, "{v}"),
            Self::Deferred(v) => write!(f, "{v}"),
        }
    }
}

impl<T> From<T> for ConfigValue<T> {
    fn from(value: T) -> Self {
        Self::Parsed(value)
    }
}

impl<T: Parse + Clone> ConfigValue<T> {
    pub(crate) fn parse(&mut self, v: impl Into<String>) {
        *self = Self::Deferred(v.into())
    }

    pub(crate) fn get(&self) -> Result<T> {
        match self {
            Self::Parsed(v) => Ok(v.clone()),
            Self::Deferred(v) => T::parse(v),
        }
    }
}

impl<T: Default> Default for ConfigValue<T> {
    fn default() -> Self {
        Self::Parsed(T::default())
    }
}

/// A value that can be stored in [`ConfigValue`]
pub(crate) trait Parse: Sized {
    fn parse(v: &str) -> Result<Self>;
}

impl Parse for bool {
    fn parse(v: &str) -> Result<Self> {
        let lower = v.to_ascii_lowercase();
        match lower.as_str() {
            "1" | "true" | "on" | "yes" | "y" => Ok(true),
            "0" | "false" | "off" | "no" | "n" => Ok(false),
            _ => Err(Error::Generic {
                store: "Config",
                source: format!("failed to parse \"{v}\" as boolean").into(),
            }),
        }
    }
}

impl Parse for Duration {
    fn parse(v: &str) -> Result<Self> {
        parse_duration(v).ok_or_else(|| Error::Generic {
            store: "Config",
            source: format!("failed to parse \"{v}\" as Duration").into(),
        })
    }
}

/// Parses a duration such as `60s`, `1h 30m`, `1.5d` or `1y 1M`
///
/// Accepts the units and lengths of `humantime`, which this replaced. A month
/// (`M`) is 30.44 days, a year 365.25 days, and every unit takes a fraction.
fn parse_duration(v: &str) -> Option<Duration> {
    const NS: u128 = 1_000_000_000;
    fn split(s: &str, f: fn(char) -> bool) -> (&str, &str) {
        s.split_at(s.find(|c| !f(c)).unwrap_or(s.len()))
    }

    let mut rest = v.trim();
    if rest == "0" {
        return Some(Duration::ZERO);
    }
    let mut total = 0u128;
    loop {
        let (int, tail) = split(rest, |c| c.is_ascii_digit());
        let (frac, tail) = match tail.strip_prefix('.') {
            Some(tail) => split(tail, |c| c.is_ascii_digit()),
            None => ("0", tail),
        };
        let (unit, tail) = split(tail.trim_start(), char::is_alphabetic);
        let unit = match unit {
            "nanos" | "nsec" | "ns" => 1,
            "usec" | "us" | "µs" => 1_000,
            "millis" | "msec" | "ms" => 1_000_000,
            "seconds" | "second" | "secs" | "sec" | "s" => NS,
            "minutes" | "minute" | "min" | "mins" | "m" => 60 * NS,
            "hours" | "hour" | "hr" | "hrs" | "h" => 3_600 * NS,
            "days" | "day" | "d" => 86_400 * NS,
            "weeks" | "week" | "wk" | "wks" | "w" => 604_800 * NS,
            "months" | "month" | "M" => 2_630_016 * NS,
            "years" | "year" | "yr" | "yrs" | "y" => 31_557_600 * NS,
            _ => return None,
        };
        // value = int.frac * unit, exact to the nanosecond or refused
        let den = 10u128.checked_pow(frac.len() as u32)?;
        let num = u128::from(int.parse::<u64>().ok()?).checked_mul(den)?;
        let value = num.checked_add(frac.parse().ok()?)?.checked_mul(unit)?;
        total = total.checked_add(Some(value).filter(|v| v % den == 0)? / den)?;
        rest = tail.trim_start();
        if rest.is_empty() {
            let secs = u64::try_from(total / NS).ok()?;
            return Some(Duration::new(secs, (total % NS) as u32));
        }
    }
}

impl Parse for usize {
    fn parse(v: &str) -> Result<Self> {
        Self::from_str(v).map_err(|_| Error::Generic {
            store: "Config",
            source: format!("failed to parse \"{v}\" as usize").into(),
        })
    }
}

impl Parse for u32 {
    fn parse(v: &str) -> Result<Self> {
        Self::from_str(v).map_err(|_| Error::Generic {
            store: "Config",
            source: format!("failed to parse \"{v}\" as u32").into(),
        })
    }
}

impl Parse for HeaderValue {
    fn parse(v: &str) -> Result<Self> {
        Self::from_str(v).map_err(|_| Error::Generic {
            store: "Config",
            source: format!("failed to parse \"{v}\" as HeaderValue").into(),
        })
    }
}

pub(crate) fn fmt_duration(duration: &ConfigValue<Duration>) -> String {
    match duration {
        ConfigValue::Parsed(v) => SpanPrinter::new().unsigned_duration_to_string(v),
        ConfigValue::Deferred(v) => v.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_parse_duration() {
        let duration = Duration::from_secs(60);
        assert_eq!(Duration::parse("60 seconds").unwrap(), duration);
        assert_eq!(Duration::parse("60 s").unwrap(), duration);
        assert_eq!(Duration::parse("60s").unwrap(), duration)
    }

    #[test]
    fn test_parse_duration_formats() {
        // Expected values match what `humantime` 2.x returned for the same inputs
        let cases = [
            ("0", Duration::ZERO),
            ("0s", Duration::ZERO),
            ("  5s\n", Duration::from_secs(5)),
            ("1.5s", Duration::from_millis(1500)),
            ("500ms", Duration::from_millis(500)),
            ("500msec", Duration::from_millis(500)),
            ("10us", Duration::from_micros(10)),
            ("10µs", Duration::from_micros(10)),
            ("5ns", Duration::from_nanos(5)),
            ("2 mins", Duration::from_secs(120)),
            ("1h 30m", Duration::from_secs(5400)),
            ("1h30m", Duration::from_secs(5400)),
            ("1.5h", Duration::from_secs(5400)),
            ("3 hrs", Duration::from_secs(10800)),
            ("1d", Duration::from_secs(86_400)),
            ("2 days", Duration::from_secs(172_800)),
            ("1 day 2 hours", Duration::from_secs(93_600)),
            ("3 weeks", Duration::from_secs(1_814_400)),
            ("1month", Duration::from_secs(2_630_016)),
            ("1y", Duration::from_secs(31_557_600)),
            (
                "1y 2months 3w 4d 5h 6m 7s 8ms",
                Duration::new(38_995_999, 8_000_000),
            ),
            ("18446744073709551615s", Duration::from_secs(u64::MAX)),
            ("1M", Duration::from_secs(2_630_016)),
            ("1y 1M", Duration::from_secs(34_187_616)),
            ("0.5d", Duration::from_secs(43_200)),
            ("1.5months", Duration::from_secs(3_945_024)),
            ("2 weeks 1.5d", Duration::from_secs(1_339_200)),
            ("1.5h 30m", Duration::from_secs(7_200)),
            ("1s 1h", Duration::from_secs(3_601)),
        ];
        for (input, expected) in cases {
            assert_eq!(Duration::parse(input).unwrap(), expected, "{input}");
        }

        for input in [
            "", "5", "s", "-5s", "-1d", "5 S", "PT5S", "1d -1h", "1.s", "1.5ns",
        ] {
            assert!(Duration::parse(input).is_err(), "{input}");
        }
    }

    #[test]
    fn test_fmt_duration_roundtrip() {
        let cases = [
            (Duration::ZERO, "0s"),
            (Duration::from_millis(1500), "1s 500ms"),
            (Duration::from_secs(90), "1m 30s"),
            (Duration::from_secs(5400), "1h 30m"),
            (Duration::from_secs(90_061), "25h 1m 1s"),
            (Duration::from_secs(u64::MAX), "5124095576030431h 15s"),
        ];
        for (duration, expected) in cases {
            let formatted = fmt_duration(&ConfigValue::Parsed(duration));
            assert_eq!(formatted, expected);
            assert_eq!(Duration::parse(&formatted).unwrap(), duration);
        }
    }
}
