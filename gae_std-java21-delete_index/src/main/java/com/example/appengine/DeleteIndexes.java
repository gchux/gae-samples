/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.example.appengine;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;

import java.io.IOException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import com.google.appengine.api.search.SearchServiceFactory;
import com.google.appengine.api.search.SearchService;
import com.google.appengine.api.search.GetIndexesRequest;
import com.google.appengine.api.search.GetRequest;
import com.google.appengine.api.search.GetResponse;
import com.google.appengine.api.search.Index;
import com.google.appengine.api.search.Document;

import com.google.appengine.api.ThreadManager;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;

import io.github.bucket4j.Bucket;

import static java.time.Duration.ofSeconds;

public class DeleteIndexes extends HttpServlet {

  private static final AtomicLong PROCESSED_DOCUMENTS = new AtomicLong(0l);
  private static final AtomicLong DELETED_DOCUMENTS = new AtomicLong(0l);

  private static final int PAGE_SIZE = 100;
  private static final int MAX_RETRIES = 5;
  private static final Long QPS = 10l;

  private static final String QPS_QUERY_PARAM = "qps";

  @Override
  public void doGet(
    HttpServletRequest request,
    HttpServletResponse response
  ) throws IOException {
    
    response.setContentType("text/plain");
    
    try {
      final ListeningExecutorService SERVICE = MoreExecutors.listeningDecorator(
        Executors.newFixedThreadPool(100, ThreadManager.currentRequestThreadFactory()));

      // control `QPS` to avoid: https://cloud.google.com/appengine/docs/standard/quotas#When_a_Resource_is_Depleted
      Long qps = QPS;
      // allow to regulate QPS against Datastore API
      final String qpsQueryParam = request.getParameter(QPS_QUERY_PARAM);
      if ( qpsQueryParam != null && !qpsQueryParam.isEmpty() ) {
        qps = Longs.tryParse(qpsQueryParam, 10);
        // if no query string parameter `qps` is available, defaults to `QPS`
        if ( qps == null ) {
          qps = QPS;
        }
      }

      final long API_QPS = qps.longValue();
      // no matter how many threads are available in the pool, no more than `QPS` should ever get to perform an API operation.
      final Bucket bucket = Bucket.builder().withNanosecondPrecision()
        .addLimit(limit -> limit.capacity(API_QPS).refillIntervally(API_QPS, ofSeconds(1)))
        .build();

      final SearchService search = SearchServiceFactory.getSearchService();

      // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.SearchService
      final GetIndexesRequest getIndexesRequest = GetIndexesRequest.newBuilder().setAllNamespaces(true).build();
      final GetResponse<Index> getIndexesResponse = search.getIndexes(getIndexesRequest);

      final WaitGroup wg = new WaitGroup();
      int indexes = 0;
      for (final Index index : getIndexesResponse) {
        indexes += 1;
        System.out.println("Deleting index: " + index.getNamespace() + "::" + index.getName());
        final DeleteIndex task = new DeleteIndex(SERVICE, wg, index, bucket);
        wg.add();
        final ListenableFuture<Long> deletedIndex = SERVICE.submit(task);
        Futures.addCallback(deletedIndex, new OnIndexDeleted(wg, task), SERVICE);
      }
      wg.await();

      final String outcome = Integer.toString(indexes, 10) + " | " + Long.toString(PROCESSED_DOCUMENTS.get(), 10) + " | " + Long.toString(DELETED_DOCUMENTS.get(), 10);

      System.out.println("DONE: " + outcome);

      response.getWriter().println(outcome);
    } catch(Exception e) {
      e.printStackTrace(System.err);
      response.getWriter().println("KO");
    }
  }

  private static class WaitGroup {

    private final Phaser phaser = new Phaser(1);

    public void add() {
      this.phaser.register();
    }

    public void add(int parties) {
      this.phaser.bulkRegister(parties);
    } 

    public void done() {
      this.phaser.arrive();
    }

    public void await() {
      this.phaser.arriveAndAwaitAdvance();
    }

  }

  private static class OnIndexDeleted implements FutureCallback<Long> {

    private final WaitGroup wg;
    private final Index index;

    OnIndexDeleted(
      final WaitGroup wg,
      final DeleteIndex task
    ) {
      this.wg = wg;
      this.index = task.getIndex();
    }

    public void onSuccess(Long processedDocuments) {
      final long overallProcessedDocuments = PROCESSED_DOCUMENTS.addAndGet(processedDocuments.longValue());
      System.out.println("processed documents from index '" + this.index.getNamespace() + "::" + this.index.getName() + 
        "' => " + Long.toString(processedDocuments.longValue(), 10) + " | overall processed documents: " + Long.toString(overallProcessedDocuments, 10)); 
      this.wg.done();
    }

    public void onFailure(Throwable thrown) {
      thrown.printStackTrace(System.err);
      this.wg.done();
    }

  }

  private static class OnIndexedDocumentsDeleted implements FutureCallback<Long> {

    private final WaitGroup wg;
    private final long iteration;
    private final Index index;

    OnIndexedDocumentsDeleted(
      final WaitGroup wg,
      final DeleteIndexedDocuments task
    ) {
      this.wg = wg;
      this.iteration = task.getIteration();
      this.index = task.getIndex();
    }

    public void onSuccess(Long deletedDocuments) {
      final long overallDeleterDocuments = DELETED_DOCUMENTS.addAndGet(deletedDocuments.longValue());
      System.out.println("deleted documents from index '" + this.index.getNamespace() + "::" + this.index.getName() + "'" + 
        "@iteration:" + Long.toString(this.iteration, 10) + " => " + Long.toString(deletedDocuments.longValue(), 10) + 
        "| overall deleted documents: " + Long.toString(overallDeleterDocuments, 10));
      this.wg.done();
    }

    public void onFailure(Throwable thrown) {
      thrown.printStackTrace(System.err);
      this.wg.done();
    }

  }

  private static class DeleteIndex implements Callable<Long> {

    private final ListeningExecutorService service;
    private final WaitGroup wg;
    private final Index index;
    private final Bucket bucket;
    private final String error;

    DeleteIndex(
      final ListeningExecutorService SERVICE,
      final WaitGroup wg,
      final Index index,
      final Bucket bucket
    ) {
      this.service = SERVICE;
      this.wg = wg;
      this.index = index;
      this.bucket = bucket;
      this.error = "[" + this.index.getNamespace() + "::" + this.index.getName() + "]/iteration:";
    }

    Index getIndex() {
      return this.index;
    }

    public Long call() throws Exception {

      final String indexNamespace = this.index.getNamespace();
      final String indexName = this.index.getName();
      
      long iteration = 0l;
      long sizeOfAllDocuments = 0l;
      int retries =  0;

      String startID = null;
      while (true) {

        try {
          this.bucket.asBlocking().consume(1);
        } catch(Exception e) {
          retries += 1;
          if (retries >= MAX_RETRIES) {
            this.wg.done();
            throw new ExecutionException("index.getRange" + this.error + Long.toString(iteration, 10) + " => MAX_RETRIES", e);
          }
          continue;
        }
        retries = 0;
        
        // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.Index
        // get netxt batch of document IDs to be deleted
        final GetRequest.Builder getDocumentsRequestBuilder = GetRequest.newBuilder().setLimit(PAGE_SIZE).setReturningIdsOnly(true);
        if ( iteration > 0l && startID != null && !startID.isEmpty() ) {
          // in order to make this loop concurrent: `getRange` operations must start and next document id
          getDocumentsRequestBuilder.setStartId(startID).setIncludeStart(false);
        }

        final GetResponse<Document> getDocumentsResponse = this.index.getRange(getDocumentsRequestBuilder.build());
        final List<Document> documents = Collections.unmodifiableList(getDocumentsResponse.getResults());
        
        final int sizeOfDocuments = documents.size();

        if (sizeOfDocuments == PAGE_SIZE) { // page is full
          startID = documents.get(sizeOfDocuments-1).getId();
        } else if (sizeOfDocuments == 0) { // page is empty
          // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.Index#com_google_appengine_api_search_Index_deleteSchema__
          this.index.deleteSchema(); // complete index deletion by deleting schema
          break; // no more indexed documents
        } else {
          startID = null;
        }
        
        try {
          final DeleteIndexedDocuments task = new DeleteIndexedDocuments(iteration, this.index, bucket, documents);
          this.wg.add();
          final ListenableFuture<Long> deletedDocuments = this.service.submit(task);
          Futures.addCallback(deletedDocuments, new OnIndexedDocumentsDeleted(this.wg, task), this.service);
        } catch(Exception e) {
          e.printStackTrace(System.err);
          System.err.println(this.error + Long.toString(iteration, 10) + " => " + e.getMessage());
          this.wg.done();
        } 

        iteration += 1l;
        sizeOfAllDocuments += sizeOfDocuments;
      }

      return Long.valueOf(sizeOfAllDocuments);
    }

  }

  private static class DeleteIndexedDocuments implements Function<Document, String>, Callable<Long> {

    private long iteration;
    private final Index index;
    private final Bucket bucket;
    private final List<Document> documents;
    private final String error;

    DeleteIndexedDocuments(
      final long iteration,
      final Index index,
      final Bucket bucket,
      final List<Document> documents
    ) {
      this.iteration = iteration;
      this.index = index;
      this.bucket = bucket;
      this.documents = documents;
      this.error = "index.delete[" + index.getNamespace() + "::" + index.getName() + "] | iteration:" + Long.toString(iteration, 10) + " | documents:" + Integer.toString(documents.size());
    }

    long getIteration() {
      return this.iteration;
    }

    Index getIndex() {
      return this.index;
    }

    List<Document> getDocuments() {
      return this.documents;
    }

    public Long call() throws Exception {
      try {
        this.bucket.asBlocking().consume(1);
      } catch(Exception e) {
        throw new ExecutionException(error + " | failed to acquire token", e);
      }

      final List<String> documentsIDs = Lists.transform(this.documents, this);
      try {
        // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.Index#com_google_appengine_api_search_Index_delete_java_lang_Iterable_java_lang_String__
        this.index.delete(documentsIDs);
      } catch(Exception e) {
        e.printStackTrace(System.err);
        throw new ExecutionException(error + " | failed to delete", e);
      }
      
      return Long.valueOf(documentsIDs.size());
    }

    public String apply(Document document) {
      return document.getId();
    }

  }

}
