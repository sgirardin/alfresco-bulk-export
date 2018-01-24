package org.alfresco.extensions.bulkexport.jobs;

import org.alfresco.repo.security.authentication.AuthenticationUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.*;

public class ScheduledExport implements InterruptableJob {

    Log log = LogFactory.getLog(ScheduledExport.class);
    private ScheduledExportExecuter executor;
    private JobExecutionContext context;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        final JobDataMap jobData = jobExecutionContext.getJobDetail().getJobDataMap();

        // Extract the Job executer to use
        Object executerObj = jobData.get("jobExecuter");
        executor = (ScheduledExportExecuter) executerObj;

        //Extract parameters
        final String nodeRef = jobData.getString("nodeRef");
        final String exportPath = jobData.getString("exportBasePath");
        final boolean ignoreExported = jobData.getBooleanFromString("ignoreExported");
        final boolean exportVersion = jobData.getBooleanFromString("exportVersion");
        final int noOfThreads = jobData.getIntegerFromString("noOfThreads");
        final int exportChunkSize = jobData.getIntegerFromString("exportChunkSize");
        final boolean useNodeCache = jobData.getBooleanFromString("useNodeCache");


        AuthenticationUtil.runAs(new AuthenticationUtil.RunAsWork<Object>() {
            public Object doWork() throws Exception {
                if (!jobData.getBooleanFromString("interrupt")) {
                    executor.execute(nodeRef, exportPath, ignoreExported, exportVersion, noOfThreads, exportChunkSize, useNodeCache);
                } else {
                    interrupt();
                }
                return null;
            }
        }, AuthenticationUtil.getSystemUserName());
    }

    @Override
    public void interrupt() throws UnableToInterruptJobException {
        log.info("Interrupting job");
        executor.interrupt();
    }
}
