//! # `emit_elasticsearch`
//! 
//! Log events with the [`emit`](http://emit-rs.github.io/emit/emit/index.html) structured logger to Elasticsearch.
//! 
//! # Usage
//! 
//! This crate is on [crates.io](http://crates.io/).

#[macro_use]
extern crate emit;
extern crate elastic_hyper as elastic;
extern crate chrono;
extern crate hyper;

use std::str;
use std::io::{ Write, Cursor };
use std::error::Error;
use emit::events::Event;
use emit::collectors::AcceptEvents;
use emit::formatters::WriteEvent;
use emit::formatters::json::RenderedJsonFormatter;
use chrono::{ DateTime, UTC };
use hyper::header::{ Headers, Authorization };
use elastic::RequestParams;

const TYPENAME: &'static str = "emitlog";
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
/// use emit_elasticsearch::IndexTemplate;
/// 
/// let template = IndexTemplate::new("emitlog-", "%Y-%m-%d");
/// ```
/// 
/// Use the `date_format` parameter to control how the event date is resolved to an index name.
/// For example, an index template that produces a unique index name for each month:
/// 
/// ```
/// # use emit_elasticsearch::IndexTemplate;
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
/// 
/// Logs are written to an index based on their `timestamp` and the given `IndexTemplate`.
pub struct ElasticCollector {
    params: RequestParams,
    template: IndexTemplate
}

unsafe impl Sync for ElasticCollector { }

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

    /// Send an index template request to Elasticsearch.
    /// 
    /// It's important to call this the before any indices are created, otherwise timestamps
    /// will be mapped as `string` instead of `date`.
    pub fn send_template(self) -> Result<ElasticCollector, Box<Error>> {
        let payload = build_index_template(&self.template);

        let mut client = hyper::Client::new();
        let res = elastic::indices::put_template::put_name(&mut client, &self.params, "emitlog", &payload[..]);

        match res {
            Ok(_) => Ok(self),
            Err(e) => Err(From::from(e))
        }
    }

    /// Send a `_bulk` request represented as a byte buffer to the given node.
    fn send_batch(&self, payload: &[u8]) -> Result<(), Box<Error>> {
        let mut client = hyper::Client::new();
        let res = elastic::bulk::post(&mut client, &self.params, payload);

        match res {
            Ok(_) => Ok(()),
            Err(e) => Err(From::from(e))
        }
    }
}

fn build_batch(events: &[Event<'static>], template: &IndexTemplate) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());
    let formatter = RenderedJsonFormatter::new();

    for evt in events {
        let idx = template.index(&evt.timestamp());

        //Writes a header struct of the form: {"index":{"_index":"{}","_type":"{}"}}\n
        buf.write_all(b"{\"index\":{\"_index\":\"").unwrap();
        buf.write_all(idx.as_bytes()).unwrap();
        buf.write_all(b"\",\"_type\":\"").unwrap();
        buf.write_all(TYPENAME.as_bytes()).unwrap();
        buf.write_all(b"\"}}\n").unwrap();

        //Writes the message body to the buffer
        formatter.write_event(&evt, &mut buf).unwrap();
        buf.write(b"\n").unwrap();
    }

    buf.into_inner()
}

fn build_index_template(template: &IndexTemplate) -> Vec<u8> {
    let mut buf = Cursor::new(Vec::new());

    //Writes a body like {"template":"testlog-*","mappings":{"emitlog":{"properties":{"@t":{"type":"date","format":"yyyy-MM-ddTHH:mm:SSSZ"}}}}}
    buf.write_all(b"{\"template\":\"").unwrap();
    buf.write_all(template.prefix.as_bytes()).unwrap();
    buf.write_all(b"*\",\"mappings\":{\"").unwrap();
    buf.write_all(TYPENAME.as_bytes()).unwrap();
    buf.write_all(b"\":{\"properties\":{\"@t\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"}}}}}").unwrap();

    buf.into_inner()
}

impl Default for ElasticCollector {
    fn default() -> ElasticCollector {
        ElasticCollector::new_local(IndexTemplate::default())
    }
}

impl AcceptEvents for ElasticCollector {
    fn accept_events(&self, events: &[Event<'static>])-> Result<(), Box<Error>> {
        let buf = build_batch(events, &self.template);
        
        self.send_batch(&buf)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::str;
    use std::collections;
    use chrono::UTC;
    use chrono::offset::TimeZone;
    use emit::{ events, templates, LogLevel, PipelineBuilder };
    use super::{ IndexTemplate, build_batch, build_index_template, ElasticCollector };

    #[test]
    fn events_are_formatted_as_bulk() {
        let template = IndexTemplate::default();
        let timestamp = UTC.ymd(2014, 7, 8).and_hms(9, 10, 11);

        let mut properties = collections::BTreeMap::new();
        properties.insert("number", "42".into());

        let evts = vec![
            events::Event::new(timestamp, LogLevel::Warn, templates::MessageTemplate::new("The number is {number}"), properties),
            events::Event::new(timestamp, LogLevel::Info, templates::MessageTemplate::new("The number is {number}"), collections::BTreeMap::new())
        ];

        let bulk = build_batch(&evts, &template);

        assert_eq!(str::from_utf8(&bulk).unwrap(), "{\"index\":{\"_index\":\"emitlog-20140708\",\"_type\":\"emitlog\"}}\n{\"@t\":\"2014-07-08T09:10:11.000Z\",\"@m\":\"The number is 42\",\"@i\":\"ae9bf784\",\"@l\":\"WARN\",\"number\":42}\n{\"index\":{\"_index\":\"emitlog-20140708\",\"_type\":\"emitlog\"}}\n{\"@t\":\"2014-07-08T09:10:11.000Z\",\"@m\":\"The number is \",\"@i\":\"ae9bf784\"}\n");
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

    #[test]
    fn can_build_index_template() {
        let template = IndexTemplate::new("testlog-", "%Y%m%d");

        let index = build_index_template(&template);

        assert_eq!(str::from_utf8(&index).unwrap(), "{\"template\":\"testlog-*\",\"mappings\":{\"emitlog\":{\"properties\":{\"@t\":{\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\"}}}}}")
    }

    #[test]
    fn pipeline_example() {
        let _flush = PipelineBuilder::new()
            .write_to(
                ElasticCollector::new_local(IndexTemplate::default()).send_template().unwrap()
            )
            .init();

        info!("Hello, {} at {} in {}!", name: env::var("USERNAME").unwrap_or("User".to_string()), time: 2139, room: "office");
    }
}
