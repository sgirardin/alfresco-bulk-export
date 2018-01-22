package org.alfresco.extensions.bulkexport.jobs;

import org.alfresco.extensions.bulkexport.StopWatch;
import org.alfresco.extensions.bulkexport.controler.CacheGeneratedException;
import org.alfresco.extensions.bulkexport.controler.Engine;
import org.alfresco.extensions.bulkexport.dao.AlfrescoExportDao;
import org.alfresco.extensions.bulkexport.dao.AlfrescoExportDaoImpl;
import org.alfresco.extensions.bulkexport.model.FileFolder;
import org.alfresco.service.ServiceRegistry;
import org.alfresco.service.cmr.repository.NodeRef;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ScheduledExportExecuter {

    Log log = LogFactory.getLog(ScheduledExportExecuter.class);

    protected ServiceRegistry serviceRegistry;

    protected AlfrescoExportDao dao;

    protected FileFolder fileFolder;

    protected Engine engine;

    public void setServiceRegistry(ServiceRegistry serviceRegistry) {
        this.serviceRegistry = serviceRegistry;
    }

    /**
     * Executer implementation
     * @param nodeRef
     * @param exportPath
     * @param ignoreExported
     * @param exportVersion
     * @param noOfThreads
     * @param exportChunkSize
     * @param useNodeCache
     */
    public void execute(String nodeRef, String exportPath, boolean ignoreExported, boolean exportVersion, int noOfThreads, int exportChunkSize, boolean useNodeCache) {
        log.info("Running the scheduled job");

        log.debug("execute scheduled job");

        StopWatch timer = new StopWatch();

        boolean revisionHead = false;

        //init variables
        dao = new AlfrescoExportDaoImpl(this.serviceRegistry);
        fileFolder = new FileFolder(exportPath, ignoreExported);
        engine = new Engine(dao, fileFolder, exportVersion, revisionHead, useNodeCache, noOfThreads, exportChunkSize);

        NodeRef nf = null;


        log.info("Bulk Export started");

        try
        {
            nf = dao.getNodeRef(nodeRef);
            engine.execute(nf);
        }
        catch (CacheGeneratedException e)
        {
            log.info("No Export performed - Cache file generated only - re-run to use cache file");
        }
        catch (Exception e)
        {
            log.error("Error found during Export (Reason): " + e.toString() + "\n");
        }

        try {
            //
            // writes will not appear until the script is finished, flush does not help
            //
            log.info("Bulk Export finished");
        }catch (Throwable e){
            log.error("Error when finishing Export (Reason): " + e.toString() + "\n");
        }
    }

    public void interrupt() {
        engine.interrupt();
    }
}
