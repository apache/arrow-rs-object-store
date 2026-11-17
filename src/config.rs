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
use jiff::fmt::friendly::{SpanParser, SpanPrinter};
use jiff::{SignedDuration, Span};

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

/// Parses a duration in jiff's "friendly" format, e.g. `60s`, `1h 30m` or `2 days`
///
/// Calendar units are converted with the fixed lengths previously used by `humantime`:
/// a day is 24 hours, a week 7 days, a month 30.44 days and a year 365.25 days.
fn parse_duration(v: &str) -> Option<Duration> {
    const PARSER: SpanParser = SpanParser::new();
    const DAY: u64 = 86_400;

    let v = v.trim();
    if v == "0" {
        return Some(Duration::ZERO);
    }
    if let Ok(d) = PARSER.parse_unsigned_duration(v) {
        return Some(d);
    }

    // Spans with calendar units (days and above) cannot be parsed into a duration directly
    let span: Span = PARSER.parse_span(v).ok()?;
    if span.is_negative() {
        return None;
    }
    let calendar_secs = (span.get_years() as u64 * 31_557_600)
        + (span.get_months() as u64 * 2_630_016)
        + (span.get_weeks() as u64 * 7 * DAY)
        + (span.get_days() as u64 * DAY);
    let time = span.years(0).months(0).weeks(0).days(0);
    let time = Duration::try_from(SignedDuration::try_from(time).ok()?).ok()?;
    time.checked_add(Duration::from_secs(calendar_secs))
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
        ];
        for (input, expected) in cases {
            assert_eq!(Duration::parse(input).unwrap(), expected, "{input}");
        }

        for input in ["", "5", "s", "-5s", "-1d", "5 S", "PT5S", "1d -1h"] {
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
