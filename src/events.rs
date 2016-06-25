use serde;
use std::collections;
use chrono::{ DateTime, UTC };
use log::LogLevel;
use elastic_types::mapping::prelude::*;
use elastic_types::date::prelude::*;
use emit::events;

pub struct ElasticLog<'a> {
    timestamp: ElasticDate<EpochMillis>,
    level: LogLevel,
    message_template: &'a str,
    properties: &'a collections::BTreeMap<&'a str, events::Value>
}

impl <'a> ElasticLog<'a> {
    pub fn new<'b>(event: &'a events::Event<'b>) -> ElasticLog<'a> {
        ElasticLog {
            timestamp: ElasticDate::new(event.timestamp()),
            level: event.level(),
            message_template: event.message_template().text(),
            properties: event.properties()
        }
    }
}

impl <'a> serde::Serialize for ElasticLog<'a> {
    fn serialize<S>(&self, serializer: &mut S) -> Result<(), S::Error>
     where S: serde::Serializer {
         panic!("implement")
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
    fn visit<S>(&mut self, serializer: &mut S) -> Result<Option<()>, S::Error>
     where S: serde::Serializer {
            try!(serializer.serialize_struct_elt("timestamp", ElasticDate::<EpochMillis>::mapping()));
            try!(serializer.serialize_struct_elt("level", String::mapping()));
            try!(serializer.serialize_struct_elt("message_template", String::mapping()));
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
        "emitlog"
    }
}

impl ElasticUserTypeMapping for ElasticLogMapping {
    type Visitor = ElasticUserTypeMappingVisitor<ElasticLogObjectVisitor>;
}
