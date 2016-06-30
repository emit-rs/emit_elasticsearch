use serde;
use std::collections;
use chrono::{ DateTime, UTC };
use log::LogLevel;
use elastic_types::mapping::prelude::*;
use elastic_types::date::prelude::*;
use emit::events;
use emit::templates::repl::MessageTemplateRepl;

const TYPENAME: &'static str = "emitlog";

pub struct ElasticLog<'a> {
	timestamp: ElasticDate<EpochMillis>,
	level: LogLevel,
	message: String,
	properties: &'a collections::BTreeMap<&'a str, events::Value>
}

impl <'a> ElasticLog<'a> {
	pub fn new<'b>(event: &'a events::Event<'b>) -> ElasticLog<'a> {
		let repl = MessageTemplateRepl::new(event.message_template().text());
		let msg = repl.replace(event.properties());

		ElasticLog {
			timestamp: ElasticDate::new(event.timestamp()),
			level: event.level(),
			message: msg,
			properties: event.properties()
		}
	}
}

impl <'a> serde::Serialize for ElasticLog<'a> {
	fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where 
	S: serde::Serializer {
		serializer.serialize_struct("", ElasticLogVisitor::new(self))
	}
}

pub struct ElasticLogVisitor<'a> {
	data: &'a ElasticLog<'a>
}
impl <'a> ElasticLogVisitor<'a> {
	fn new(data: &'a ElasticLog<'a>) -> Self {
		ElasticLogVisitor {
			data: data
		}
	}
}

impl <'a> serde::ser::MapVisitor for ElasticLogVisitor<'a> {
	fn visit<S>(&mut self, serializer: &mut S) -> Result<Option<()>, S::Error> where 
	S: serde::Serializer {
		try!(serializer.serialize_struct_elt("@t", &self.data.timestamp));
		try!(serializer.serialize_struct_elt("@lvl", self.data.level.to_string()));
		try!(serializer.serialize_struct_elt("msg", &self.data.message));

		//TODO: impl serde::Serialize for events::Value
		//try!(serializer.serialize_struct_elt("props", self.data.properties));

		Ok(None)
	}
}

impl <'a> ElasticType<ElasticLogMapping, ()> for ElasticLog<'a> { }

#[derive(Default, Clone)]
pub struct ElasticLogObjectVisitor;
impl ElasticTypeVisitor for ElasticLogObjectVisitor {
	fn new() -> Self {
		ElasticLogObjectVisitor
	}
}

impl serde::ser::MapVisitor for ElasticLogObjectVisitor {
	fn visit<S>(&mut self, serializer: &mut S) -> Result<Option<()>, S::Error> where 
	S: serde::Serializer {
		try!(serializer.serialize_struct_elt("@t", ElasticDate::<EpochMillis>::mapping()));
		try!(serializer.serialize_struct_elt("@lvl", String::mapping()));
		try!(serializer.serialize_struct_elt("msg", String::mapping()));
		Ok(None)
	}
}

#[derive(Default, Clone)]
pub struct ElasticLogMapping;
impl serde::Serialize for ElasticLogMapping {
	fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error> where
	S: serde::Serializer {
		serializer.serialize_struct("", Self::get_visitor())
	}
}

impl ElasticObjectMapping for ElasticLogMapping {
	fn data_type() -> &'static str {
		DYNAMIC_DATATYPE
	}
}
impl ElasticFieldMapping<()> for ElasticLogMapping {
	type Visitor = ElasticObjectMappingVisitor<ElasticLogMapping, ElasticLogObjectVisitor>;

	fn data_type() -> &'static str {
		<Self as ElasticObjectMapping>::data_type()
	}

	fn name() -> &'static str {
		TYPENAME
	}
}

impl ElasticUserTypeMapping for ElasticLogMapping {
	type Visitor = ElasticUserTypeMappingVisitor<ElasticLogObjectVisitor>;
}

#[cfg(test)]
mod tests {
	use std::collections;
	use serde_json;
	use chrono::UTC;
	use chrono::offset::TimeZone;
	use log;
	use emit::{ events, templates };
	use super::ElasticLog;
	use ::IndexTemplate;

	#[test]
	fn events_are_formatted() {
		let template = IndexTemplate::default();
		let timestamp = UTC.ymd(2014, 7, 8).and_hms(9, 10, 11);

		let mut properties = collections::BTreeMap::new();
		properties.insert("number", "42".into());

		let evt = events::Event::new(timestamp, log::LogLevel::Warn, templates::MessageTemplate::new("The number is {number}"), properties);
		let es_evt = ElasticLog::new(&evt);

		let formatted = serde_json::to_string(&es_evt).unwrap();

		assert_eq!(r#"{}"#, &formatted);
	}
}