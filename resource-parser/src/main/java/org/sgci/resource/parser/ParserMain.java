/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.sgci.resource.parser;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ListBranchCommand;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.transport.FetchResult;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

import java.io.File;
import java.io.FileReader;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@SpringBootApplication()
@ComponentScan(basePackages = {"org.sgci.resource"})
public class ParserMain  implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(ParserMain.class);

    @org.springframework.beans.factory.annotation.Value("${mongo.connection.url}")
    private String mongoConnectionUrl;

    @org.springframework.beans.factory.annotation.Value("${mongo.db.name}")
    private String mongoDbName;

    @org.springframework.beans.factory.annotation.Value("${mongo.collection.name}")
    private String mongoCollectionName;

    @org.springframework.beans.factory.annotation.Value("${git.resources.repo}")
    private String repoUrl;

    @org.springframework.beans.factory.annotation.Value("${local.repo.dir}")
    private String repoDir;

    private final ScheduledExecutorService monitorPool = Executors.newSingleThreadScheduledExecutor();
    private String lastErroredCommitId = null;

    private Git cloneRepo(String repoUrl, String repoDir) throws Exception {

        File cloneDir = new File(repoDir);
        if (cloneDir.exists()) {
            FileUtils.deleteDirectory(cloneDir);
        }

        return Git.cloneRepository()
                .setURI(repoUrl)
                .setDirectory(cloneDir)
                .call();
    }

    private String getLastCommit(Git git) throws Exception {
        RevCommit youngestCommit = null;
        List<Ref> branches = git.branchList().setListMode(ListBranchCommand.ListMode.ALL).call();

        RevWalk walk = new RevWalk(git.getRepository());
        for(Ref branch : branches) {
            RevCommit commit = walk.parseCommit(branch.getObjectId());
            if(youngestCommit == null || commit.getAuthorIdent().getWhen().compareTo(
                    youngestCommit.getAuthorIdent().getWhen()) > 0)
                youngestCommit = commit;
        }

        return youngestCommit.getName();
    }

    /**
     * This goes through all the data json files in the repository and update the mongodb with the collected JSON
     * entries
     * @param repoDir
     * @param dbName
     * @param collectionName
     */
    private void updateMongo(String repoDir, String dbName, String collectionName) throws Exception {

        // TODO : Right not this updates all the mongo documents but this is very inefficient. Update only the
        // required entries by getting a diff

        logger.info("Updating mongo collection");

        MongoClient mongoClient = MongoClients.create(this.mongoConnectionUrl);

        MongoCollection<Document> collection = mongoClient.getDatabase(dbName).getCollection(collectionName);
        collection.deleteMany(new BasicDBObject());

        JSONParser jsonParser = new JSONParser();

        File dataDir = new File(repoDir + "/data");

        if (dataDir.exists() && dataDir.isDirectory()) {
            File[] children = dataDir.listFiles();

            if (children != null) {
                for (File dataFile : children) {

                    if (dataFile.isFile()) {
                        logger.info("Processing file {}", dataFile.getAbsolutePath());

                        try (FileReader reader = new FileReader(dataFile)) {
                            //Read JSON file
                            Object obj = jsonParser.parse(reader);

                            JSONArray resourceListJson = (JSONArray) ((JSONObject) obj).get("sgciResources");
                            resourceListJson.forEach(resourceJson -> {
                                collection.insertOne(Document.parse(resourceJson.toString()));
                            });

                        }
                    }
                }
            }
        }

        logger.info("Completed updating mongo collection");
    }

    /**
     * Clones the repository from remote to local direcotry and run a mongo synchronization round
     * @return
     * @throws Exception
     */
    private Git init() throws Exception {
        logger.info("Initializing the parser with repo {}", repoUrl);
        Git git = cloneRepo(repoUrl, repoDir);
        logger.info("Done cloning repo to {}", repoDir);

        updateMongo(repoDir, mongoDbName, mongoCollectionName);
        return git;
    }

    private void notifyAdmins(String commitId, Throwable error) {
        if (commitId != null) {
            if (!commitId.equals(lastErroredCommitId)) {
                lastErroredCommitId = commitId;

                error.printStackTrace();
            }
        }

        // Else ignore as this commit is already notified
    }

    @Override
    public void run(String... args) throws Exception {

        final Git git = init();

        monitorPool.scheduleWithFixedDelay(() -> {
            String lastLocalCommitName = null;
            try {
                lastLocalCommitName = getLastCommit(git);

                FetchResult fetchResult = git.fetch().call();
                String latestRemoteCommitName = fetchResult.getAdvertisedRefs().stream()
                        .filter(ref -> ref.getName().equals("HEAD"))
                        .findFirst().get().getObjectId().getName();

                boolean pullRequired = !lastLocalCommitName.equals(latestRemoteCommitName);

                logger.info("Remote commit {} Local commit {}. Pull Required? {}", latestRemoteCommitName, lastLocalCommitName, pullRequired);

                if (pullRequired) {
                    logger.info("Pulling from remote");
                    git.pull().call();
                    lastLocalCommitName = getLastCommit(git);

                    updateMongo(repoDir, mongoDbName, mongoCollectionName);
                }
            } catch (Exception e) {
                e.printStackTrace();
                notifyAdmins(lastLocalCommitName, e);
            }
        }, 2, 2, TimeUnit.SECONDS);
    }

    public static void main(String args[]) {
        SpringApplication.run(ParserMain.class);
    }
}
