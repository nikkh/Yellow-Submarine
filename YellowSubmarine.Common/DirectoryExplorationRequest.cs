using System;
using System.Collections.Generic;
using System.Text;

namespace YellowSubmarine.Common
{
    public class DirectoryExplorationRequest
    {
        public string RequestId { get; set; }
        public string StartPath { get; set; }
        public string ContinuationToken { get; set; }
        public int PageNumber { get; set; }

        public int TargetDepth { get; set; }
        public int CurrentDepth { get; set; }
        public string ETag { get; set; }
        public string ModifiedDateTime { get; set; }

        public string LastPathProcessed { get; set; }
    }
}
