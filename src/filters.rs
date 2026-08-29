use std::collections::BTreeMap;

use crate::error::FilterError;

const MAX_NAMED_FILTERS: usize = 16;
const MAX_KEYS_PER_FILTER: usize = 1000;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filter {
    pub(crate) include: Vec<String>,
    pub(crate) exclude: Vec<String>,
    pub(crate) required: Vec<String>,
}

fn validate_keys<I, S>(keys: I) -> Result<Vec<String>, FilterError>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let mut out = Vec::new();
    for key in keys {
        let key = key.into();
        let mut buf = [0u8; 32];
        match bs58::decode(&key).onto(&mut buf) {
            Ok(32) => out.push(key),
            _ => return Err(FilterError::InvalidKey(key)),
        }
    }
    Ok(out)
}

impl Filter {
    pub fn all() -> Self {
        Self::default()
    }

    pub fn new() -> Self {
        Self::default()
    }

    pub fn include<I, S>(mut self, keys: I) -> Result<Self, FilterError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.include.extend(validate_keys(keys)?);
        self.check_len()?;
        Ok(self)
    }

    pub fn exclude<I, S>(mut self, keys: I) -> Result<Self, FilterError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.exclude.extend(validate_keys(keys)?);
        self.check_len()?;
        Ok(self)
    }

    pub fn required<I, S>(mut self, keys: I) -> Result<Self, FilterError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required.extend(validate_keys(keys)?);
        self.check_len()?;
        Ok(self)
    }

    fn key_count(&self) -> usize {
        self.include.len() + self.exclude.len() + self.required.len()
    }

    fn check_len(&self) -> Result<(), FilterError> {
        if self.key_count() > MAX_KEYS_PER_FILTER {
            return Err(FilterError::TooManyKeys(String::new(), self.key_count()));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Filters(pub(crate) BTreeMap<String, Filter>);

impl Filters {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(mut self, name: impl Into<String>, filter: Filter) -> Self {
        self.0.insert(name.into(), filter);
        self
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn validate(&self) -> Result<(), FilterError> {
        if self.0.is_empty() {
            return Err(FilterError::Empty);
        }
        if self.0.len() > MAX_NAMED_FILTERS {
            return Err(FilterError::TooManyFilters(self.0.len()));
        }
        for (name, f) in &self.0 {
            if f.key_count() > MAX_KEYS_PER_FILTER {
                return Err(FilterError::TooManyKeys(name.clone(), f.key_count()));
            }
        }
        Ok(())
    }
}

impl<S: Into<String>> FromIterator<(S, Filter)> for Filters {
    fn from_iter<T: IntoIterator<Item = (S, Filter)>>(iter: T) -> Self {
        Filters(iter.into_iter().map(|(n, f)| (n.into(), f)).collect())
    }
}
