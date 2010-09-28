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
			if (t == typeof(bool)) {
				retval = "{\"type\":\"boolean\"}";
			} else if (t == null) {
				retval = "{\"type\":\"null\"}";
			} else if (t == typeof(string)) {
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
					
					var name = typeName.Key.Length > 0 ? ",\"namespace\":\"" + typeName.Value + "\"" : "";
					retval = "{\"type\":\"" + typeName.Value + "\"" + name + "}";
					
				} else {
					
					retval = GetSchemaForComplexType (t, traversedTypes);
				}
			}
			traversedTypes.Add (t);
			return retval;
		}

		private static KeyValuePair<String, String> GetTypeNameAndNamespace (Type t)
		{
			var retval = new KeyValuePair<String, String> ("", "");
			if (t == typeof(string)) {
				retval = new KeyValuePair<String, String> ("", "string");
			} else if (t == typeof(long)) {
				retval = new KeyValuePair<String, String> ("", "long");
			} else if (t == typeof(int)) {
				retval = new KeyValuePair<String, String> ("", "int");
			} else if (t == typeof(float)) {
				retval = new KeyValuePair<String, String> ("", "float");
			} else if (t == typeof(double)) {
				retval = new KeyValuePair<String, String> ("", "double");
			} else if (t == typeof(byte[]) || t == typeof(byte)) {
				retval = new KeyValuePair<String, String> ("", "bytes");
			} else if (t == typeof(Guid)) {
				retval = new KeyValuePair<String, String> ("", "guid");
			} else if (t == typeof(decimal)) {
				retval = new KeyValuePair<String, String> ("", "decimal");
			} else if (t == typeof(DateTime)) {
				retval = new KeyValuePair<String, String> ("", "utcdatetime");
			} else if (t == typeof(TimeSpan)) {
				retval = new KeyValuePair<String, String> (t.Namespace, t.Name);
				//should probably handle dictionaries and enumerables
			} else {
				retval = new KeyValuePair<String, String> (t.Namespace, t.Name);
			}
			return retval;
		}

		private static String GetSchemaForComplexType (Type t, HashSet<Type> traversedTypes)
		{
			var retval = "";
			if (t == typeof(Nullable<>)) {
				
				var generic = GetTypeNameAndNamespace (t.GetGenericArguments ().ElementAt (0));
				var name = generic.Key.Length > 0 ? generic.Key + "." + generic.Value : generic.Value;
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
					
					var name = typeName.Key.Length > 0 ? ",\"namespace\":\"" + typeName.Key + "\"" : "";
					retval = "{\"type\":\"" + typeName.Value + "\"" + name + "}";
				} else {
					//this is a user-defined type. nice.
					var typeName = GetTypeNameAndNamespace (t);
					var name = typeName.Key.Length > 0 ? ",\"namespace\":\"" + typeName.Key + "\"" : "";
					retval = "{\"type\":\"record\", \"name\":\"" + typeName.Value + "\"" + name + "\",\"fields\":[";
					
					foreach (var p in t.GetProperties (BindingFlags.GetProperty | BindingFlags.Public | BindingFlags.Instance)) {
						var propType = GetTypeNameAndNamespace (p.PropertyType);
						var ns = propType.Key.Length > 0 ? propType.Key + "." + propType.Value : propType.Value;
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

