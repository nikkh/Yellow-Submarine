using System;
using System.Collections.Generic;
using System.Text;

namespace YellowSubmarine.Common
{
    public class ExplorationResult
    {
        public ExplorationResult() 
        {
            this.ResultDateTime = DateTime.UtcNow;
        }
        public InspectionResultType Type { get; set; }
        public string Acls { get; set; }
        public string Path { get; set; }
        public string RequestId { get; set; }
        public DateTime ResultDateTime { get; }
        public string ETag { get; set; }
        public string ModifiedDateTime { get; set; }
    }

    public enum InspectionResultType { Directory, File}
}
