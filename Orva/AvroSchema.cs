using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;

namespace Orva
{
	
	public class AvroSchema
	{
		/// <summary>
		/// Produce an AVRO schema (a special JSON string)
		/// </summary>
		/// <param name="t">
		/// The type on which to produce the schema.
		/// </param>
		/// <returns>
		/// An AVRO schema.
		/// </returns>
		public static string GetSchema (Type t)
		{
			return GetSchema (t, new HashSet<Type> ());
		}

		/// <summary>
		/// Convenience overload for the other "GetSchema" call.
		/// </summary>
		/// <returns>
		/// An AVRO Schema string.
		/// </returns>
		public static string GetSchema<T> ()
		{
			return GetSchema (typeof(T));
		}
		
		private static string GetSchema (Type t, HashSet<Type> traversedTypes)
		{
			var retval = "";
			//lookup the type in the typeconverter directory.
			
			if (t == typeof(string)) {
				retval = "{\"type\":\"string\"}";
			} else if (t == typeof(long)) {
				retval = "{\"type\":\"long\"}";
			} else if (t == typeof(int)) {
				retval = "{\"type\":\"int\"}";
			} else if (t == typeof(float)) {
				retval = "{\"type\":\"float\"}";
			} else if (t == typeof(double)) {
				retval = "{\"type\":\"double\"}";
			} else if (t == typeof(byte[]) || t == typeof(byte)) {
				retval = "{\"type\":\"bytes\"}";
			} else {
				if (traversedTypes.Contains (t)) {
					var typeName = GetTypeNameAndNamespace (t);
					
					var name = typeName.Item1.Length > 0 ? ",\"namespace\":\"" + typeName.Item1 + "\"" : "";
					retval = "{\"type\":\"" + typeName.Item2 + "\"" + name + "}";
					
				} else {
					
					retval = GetSchemaForComplexType (t, traversedTypes);
				}
			}
			traversedTypes.Add (t);
			return retval;
		}

		private static Tuple<String, String> GetTypeNameAndNamespace (Type t)
		{
			var retval = Tuple.Create ("", "");
			if (t == typeof(string)) {
				retval = Tuple.Create ("", "string");
			} else if (t == typeof(long)) {
				retval = Tuple.Create ("", "long");
			} else if (t == typeof(int)) {
				retval = Tuple.Create ("", "int");
			} else if (t == typeof(float)) {
				retval = Tuple.Create ("", "float");
			} else if (t == typeof(double)) {
				retval = Tuple.Create ("", "double");
			} else if (t == typeof(byte[]) || t == typeof(byte)) {
				retval = Tuple.Create ("", "bytes");
			} else if (t == typeof(Guid)) {
				retval = Tuple.Create ("", "guid");
			} else if (t == typeof(decimal)) {
				retval = Tuple.Create ("", "decimal");
			} else if (t == typeof(DateTime)) {
				retval = Tuple.Create ("", "utcdatetime");
			} else if (t == typeof(TimeSpan)) {
				retval = Tuple.Create (t.Namespace, t.Name);
				//should probably handle dictionaries and enumerables
			} else {
				retval = Tuple.Create (t.Namespace, t.Name);
			}
			return retval;
		}

		private static String GetSchemaForComplexType (Type t, HashSet<Type> traversedTypes)
		{
			var retval = "";
			if (t == typeof(Nullable<>)) {
				
				var generic = GetTypeNameAndNamespace (t.GetGenericArguments ().ElementAt (0));
				var name = generic.Item1.Length > 0 ? generic.Item1 + "." + generic.Item2 : generic.Item2;
				retval = "[\"" + name + "\", null]";
				
			} else if (t == typeof(Guid)) {
				retval = @"{""type"":""fixed"",""size"":16, ""name"":""guid""}";
				
			} else if (t == typeof(decimal)) {
				retval = @"{""type"":""fixed"",""size"":16, ""name"":""decimal""}";
				
			} else if (t == typeof(DateTime)) {
				retval = @"{""type"":""fixed"",""size"": 8, ""name"":""utcdatetime""}";
				
			} else if (t.IsEnum) {
				
				var symbols = Enum.GetNames (t).Aggregate (new StringBuilder ("["), (seed, current) => seed.AppendFormat ("{0},", current)).ToString ().TrimEnd (',') + "]";
				retval = "{\"type\":\"enum\",\"name\":\"" + t.Name + "\",\"namespace\":\"" + t.Namespace + "\",\"" + symbols + "\"}";
				
			} else if (t == typeof(TimeSpan)) {
				throw new NotSupportedException ("will do the timespan, soon.");
			} else if (t.GetInterfaces ().Any (i => i.Name == "IEnumerable<>")) {
				throw new NotImplementedException ("will do the enumerable, soon.");
			} else if (t.GetInterfaces ().Any (f => f.Name == "IEnumerable")) {
				throw new NotImplementedException ("will do the enumerable, soon.");
			} else if (t.GetInterfaces ().Any (i => i.Name == "IDictionary<,>")) {
				throw new NotImplementedException ("will do the IDictionary/map, soon.");
			} else {
				if (traversedTypes.Contains (t)) {
					var typeName = GetTypeNameAndNamespace (t);
					
					var name = typeName.Item1.Length > 0 ? ",\"namespace\":\"" + typeName.Item1 + "\"" : "";
					retval = "{\"type\":\"" + typeName.Item2 + "\"" + name + "}";
				} else {
					//this is a user-defined type. nice.
					var typeName = GetTypeNameAndNamespace (t);
					var name = typeName.Item1.Length > 0 ? ",\"namespace\":\"" + typeName.Item1 + "\"" : "";
					retval = "{\"type\":\"record\", \"name\":\"" + typeName.Item2 + "\"" + name + "\",\"fields\":[";
					
					foreach (var p in t.GetProperties (BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance)) {
						var propType = GetTypeNameAndNamespace (p.PropertyType);
						var ns = propType.Item1.Length > 0 ? propType.Item1 + "." + propType.Item2 : propType.Item2;
						if (traversedTypes.Contains (p.PropertyType)) {
							retval += "{\"name\":\"" + p.Name + "\", \"type\":\"" + ns + "\"},";
						} else {
							retval += "{\"name\":\"" + p.Name + "\", \"type\":\"" + GetSchema (p.PropertyType, traversedTypes) + "\"},";
						}
					}
					
					retval.TrimEnd (',');
					
					retval += "]}";
				}
			}
			
			traversedTypes.Add (t);
			return retval;
		}

	}
}

