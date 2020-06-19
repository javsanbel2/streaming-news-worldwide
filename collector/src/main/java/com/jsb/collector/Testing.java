package com.jsb.collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Testing {
	public static void main(String[] args) {
		String fge = "QueryStatistics{creationTime=1592571635093, endTime=null, startTime=null, numChildJobs=null, parentJobId=null, scriptStatistics=null, billingTier=null, cacheHit=false, totalBytesBilled=0, totalBytesProcessed=2285206, queryPlan=null, timeline=null, schema=Schema{fields=[Field{name=has_downloads, type=BOOLEAN, mode=NULLABLE, description=null, policyTags=null}]}}\n";
		Matcher m = Pattern.compile("totalBytesProcessed=(\\d*)").matcher(fge);
		if (m.find()) System.out.println(m.group(1));
		
	}
}
