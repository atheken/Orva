using System;
using NUnit.Framework;
using Orva;

namespace Orva.Tests
{
	[TestFixture]
	public class SchemaTests
	{
		public SchemaTests ()
		{
		}
		
		[Test]
		public void GetSchemaForStringWorks()
		{
			Console.WriteLine(AvroSchema.GetSchema<String>());
			//Assert.AreEqual("{\"type\":\"string\"}", AvroSchema.GetSchema<String>());	
		}
	}
}

