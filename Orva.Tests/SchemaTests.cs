using System;
using NUnit.Framework;
using Orva;
using Orva.Tests;

namespace Orva.Tests
{
	[TestFixture]
	public class SchemaTests
	{
		public SchemaTests ()
		{
		}

		[Test]
		public void GetSchemaForStringWorks ()
		{
			Assert.AreEqual ("{\"type\":\"string\"}", AvroSchema.GetSchema<String> ());
		}

		[Test]
		public void GetSchemaForLongWorks ()
		{
			Assert.AreEqual ("{\"type\":\"long\"}", AvroSchema.GetSchema<long> ());
		}
		[Test]
		public void GetSchemaForIntWorks ()
		{
			Assert.AreEqual ("{\"type\":\"int\"}", AvroSchema.GetSchema<int> ());
		}
		[Test]
		public void GetSchemaForFloatWorks ()
		{
			Assert.AreEqual ("{\"type\":\"float\"}", AvroSchema.GetSchema<float> ());
		}

		[Test]
		public void GetSchemaForDoubleWorks ()
		{
			Assert.AreEqual ("{\"type\":\"double\"}", AvroSchema.GetSchema<double> ());
		}
		public void GetSchemaForBytesWorks ()
		{
			Assert.AreEqual ("{\"type\":\"bytes\"}", AvroSchema.GetSchema<byte> ());
			Assert.AreEqual ("{\"type\":\"bytes\"}", AvroSchema.GetSchema<byte[]> ());
		}
		[Test]
		public void GetSchemaForGuidWorks ()
		{
			Assert.AreEqual ("{\"type\":\"fixed\",\"size\":16, \"name\":\"guid\"}", AvroSchema.GetSchema<Guid> ());
		}

		[Test]
		public void GetSchemaForBooleanWorks ()
		{
			Assert.AreEqual ("{\"type\":\"boolean\"}", AvroSchema.GetSchema<bool> ());
		}
		[Test]
		public void GetSchemaForNullWorks ()
		{
			Assert.AreEqual ("{\"type\":\"null\"}", AvroSchema.GetSchema (null));
		}
		[Test]
		public void GetSchemaForDecimalWorks ()
		{
			Assert.AreEqual ("{\"type\":\"fixed\",\"size\":16, \"name\":\"decimal\"}", AvroSchema.GetSchema<Decimal> ());
		}
		
		[Test]
		public void GetSchemaForDateTimeWorks()
		{
			Assert.AreEqual ("{\"type\":\"fixed\",\"size\":16, \"name\":\"utcdatetime\"}", AvroSchema.GetSchema<DateTime> ());
		}
		
		[Test]
		public void GetSchemaForTimespanWorks()
		{
			Assert.AreEqual ("{\"type\":\"fixed\",\"size\":16, \"name\":\"timespan\"}", AvroSchema.GetSchema<TimeSpan> ());
		}
		
		[Test]
		public void GetSchemaForComplexTypeWorks()
		{
			throw new NotImplementedException();
			//Assert.AreEqual ("{\"type\":\"fixed\",\"size\":16, \"name\":\"timespan\"}", AvroSchema.GetSchema<ComplexType> ());
		}
	}
}

