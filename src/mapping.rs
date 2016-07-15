use serde;
use elastic_types::mapping::prelude::*;
use elastic_types::date::prelude::*;

pub const TYPENAME: &'static str = "emitlog";

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
		try!(serializer.serialize_struct_elt("@t", ElasticDate::<ChronoFormat>::mapping()));
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
	use elastic_types::mappers::TypeMapper;
	use super::ElasticLogMapping;

	#[test]
	fn timestamp_has_date_mapping() {
		let mapping = TypeMapper::to_string(ElasticLogMapping).unwrap();

		assert_eq!(&mapping, "{\"properties\":{\"@t\":{\"type\":\"date\",\"format\":\"yyyy-MM-ddTHH:mm:ssZ\"}}}");
	}
}