using System;
using System.IO;
using System.Collections;
using System.Text;

namespace com.CastRoller.AmazonS3DA
{
	public class S3StreamObject
	{
		private Stream stream;
		public Stream Stream 
		{
			get 
			{
				return stream;
			}
		}

		private SortedList metadata;
		public SortedList Metadata 
		{
			get 
			{
				return metadata;
			}
		}

		public S3StreamObject( Stream stream, SortedList metadata ) 
		{
			this.stream = stream;
			this.metadata = metadata;
		}
	}
}
