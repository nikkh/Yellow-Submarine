﻿using System;
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
    }
}
