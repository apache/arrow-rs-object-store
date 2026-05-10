use std::str::FromStr;

use url::Url;

use crate::ObjectStore;

pub(crate) trait FromUrlBuilder {
    type ConfigKey: FromStr;

    fn builder_new() -> Self;

    fn builder_with_url(self, url: Url) -> Self;

    fn builder_with_config(self, key: &str, value: String) -> Self;

    fn builder_build(self) -> crate::Result<Box<dyn ObjectStore>>;

    fn build_from_url_and_options<K, V>(
        url: &Url,
        options: impl IntoIterator<Item = (K, V)>,
    ) -> crate::Result<Box<dyn ObjectStore>>
    where
        Self: Sized,
        K: AsRef<str>,
        V: Into<String>,
    {
        let b = Self::builder_new().builder_with_url(url.clone());

        let b = options.into_iter().fold(b, |builder, (key, value)| {
            builder.builder_with_config(key.as_ref(), value.into())
        });

        b.builder_build()
    }
}
