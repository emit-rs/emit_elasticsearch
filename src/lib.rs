extern crate emit;
extern crate log;
extern crate elastic_hyper as elastic;
extern crate elastic_types;
extern crate chrono;
extern crate hyper;
extern crate serde;
extern crate serde_json;

use std::io::{ Read, Write };
use std::error::Error;
use emit::events::Event;
use emit::collectors::AcceptEvents;
use chrono::{ DateTime, UTC };
use hyper::client::Body;
use hyper::header::{ Headers, Connection, Authorization, Scheme };
use elastic::RequestParams;

mod events;

pub const LOCAL_SERVER_URL: &'static str = "http://localhost:9200/";
pub const DEFAULT_TEMPLATE_PREFIX: &'static str = "emitlog-";
pub const DEFAULT_TEMPLATE_FORMAT: &'static str = "%Y%m%d";

/// Template for naming log indices.
/// 
/// The index name consists of a prefix and a date format (`chrono` compatible).
/// The default `IndexTemplate` produces index names like `'emitlog-20160501'`.
/// 
/// # Examples
/// 
/// An index template that produces index names like `'logs-2016-05-01'`:
/// 
/// ```
/// use emit::collectors::elasticsearch::IndexTemplate;
/// 
/// let template = IndexTemplate::new("emitlog-", "%Y-%m-%d");
/// ```
/// 
/// Use the `date_format` parameter to control how the event date is resolved to an index name.
/// For example, an index template that produces a unique index name for each month:
/// 
/// ```
/// # use emit::collectors::elasticsearch::*;
/// let template = IndexTemplate::new("emitlog-", "%Y%m");
/// ```
pub struct IndexTemplate {
    prefix: String,
    date_format: String
}

impl IndexTemplate {
    pub fn new<I>(prefix: I, date_format: I) -> IndexTemplate where
    I: Into<String> {
        IndexTemplate {
            prefix: prefix.into(),
            date_format: date_format.into()
        }
    }

    pub fn index(&self, date: &DateTime<UTC>) -> String {
        let df = date.format(&self.date_format).to_string();

        let mut fmtd = String::with_capacity(self.prefix.len() + df.len());
        fmtd.push_str(&self.prefix);
        fmtd.push_str(&df);

        fmtd
    }
}

impl Default for IndexTemplate {
    fn default() -> IndexTemplate {
        IndexTemplate {
            prefix: DEFAULT_TEMPLATE_PREFIX.into(),
            date_format: DEFAULT_TEMPLATE_FORMAT.into()
        }
    }
}

/// Log collector for Elasticsearch.
pub struct ElasticCollector {
    params: RequestParams,
    template: IndexTemplate
}

impl ElasticCollector {
    /// Create a new collector with the given node url and naming template.
    pub fn new<I>(server_url: I, index_template: IndexTemplate) -> ElasticCollector where
    I: Into<String> {
        let params = RequestParams::new(server_url, Headers::new());

        ElasticCollector {
            params: params,
            template: index_template
        }
    }

    /// Create a new collector for logging to `localhost:9200`.
    pub fn new_local(index_template: IndexTemplate) -> ElasticCollector {
        Self::new(LOCAL_SERVER_URL, index_template)
    }

    /// Supply an auth header for index requests.
    pub fn with_auth(mut self, auth: String) -> ElasticCollector {
        self.params.headers.set(Authorization(auth));

        self
    }

    fn send_batch(&self, payload: &[u8]) -> Result<(), Box<Error>> {
        let mut client = hyper::Client::new();

        let res = elastic::bulk::post(&mut client, &self.params, payload);

        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(From::from(e))
        }
    }
}

impl Default for ElasticCollector {
    fn default() -> ElasticCollector {
        ElasticCollector::new_local(IndexTemplate::default())
    }
}

impl AcceptEvents for ElasticCollector {
    fn accept_events(&self, events: &[Event<'static>])-> Result<(), Box<Error>> {
        //TODO:
        //Build a buffer
        //Write events to the buffer:
        // - bulk header struct
        // - bulk event struct
        panic!("implement")
    }
}

#[cfg(test)]
mod tests {
    use std::collections;
    use chrono::UTC;
    use chrono::offset::TimeZone;
    use log;
    use ::{ events, templates };
    use super::{ IndexTemplate, ElasticCollector };

    #[test]
    fn events_are_formatted() {
        let timestamp = UTC.ymd(2014, 7, 8).and_hms(9, 10, 11);
        let mut properties: collections::BTreeMap<&'static str, String> = collections::BTreeMap::new();
        properties.insert("number", "42".to_owned());
        let evt = events::Event::new(timestamp, log::LogLevel::Warn, templates::MessageTemplate::new("The number is {number}"), properties);

        //build a bulk call from a slice of Events
        //ensure it has the correct structure
        //panic!("implement")
    }

    #[test]
    fn dates_use_correct_index() {
        let template_y = IndexTemplate::new("testlog-", "%Y");
        let template_ym = IndexTemplate::new("testlog-", "%Y%m");
        let template_ymd = IndexTemplate::new("testlog-", "%Y%m%d");

        let date = UTC.ymd(2014, 7, 8).and_hms(9, 10, 11);

        assert_eq!("testlog-2014", &template_y.index(&date));
        assert_eq!("testlog-201407", &template_ym.index(&date));
        assert_eq!("testlog-20140708", &template_ymd.index(&date));
    }
}
