using System;
using System.Collections.Generic;
using System.Text;

namespace YellowSubmarine
{
    public class ExplorationResult
    {
        public ExplorationResult() 
        {
            this.DateTime = DateTime.UtcNow;
        }
        public InspectionResultType Type { get; set; }
        public string Acls { get; set; }
        public string Path { get; set; }
        public string RequestId { get; set; }
        public DateTime DateTime { get; }
    }

    public enum InspectionResultType { Directory, File}
}
