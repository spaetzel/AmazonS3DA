// This software code is made available "AS IS" without warranties of any        
// kind.  You may copy, display, modify and redistribute the software            
// code either by itself or as incorporated into your code; provided that        
// you do not remove any proprietary notices.  Your use of this software         
// code is at your own risk and you waive any claim against Amazon               
// Digital Services, Inc. or its affiliates with respect to your use of          
// this software code. (c) 2006 Amazon Digital Services, Inc. or its             
// affiliates.          

using System;
using System.Text;
using System.Xml;

namespace com.CastRoller.AmazonS3DA
{
    public class Bucket
    {
        private string name;
        public string Name
        {
            get
            {
                return name;
            }
        }

        private DateTime creationDate;
        public DateTime CreationDate
        {
            get
            {
                return creationDate;
            }
        }

        public Bucket(string name, DateTime creationDate)
        {
            this.name = name;
            this.creationDate = creationDate;
        }

        public Bucket(XmlNode node)
        {
            foreach (XmlNode child in node.ChildNodes)
            {
                if (child.Name.Equals("Name"))
                {
                    name = Utils.getXmlChildText(child);
                }
                else if (child.Name.Equals("CreationDate"))
                {
                    string strDate = Utils.getXmlChildText(child);
                    creationDate = Utils.parseDate(strDate);
                }
            }
        }
    }
}
