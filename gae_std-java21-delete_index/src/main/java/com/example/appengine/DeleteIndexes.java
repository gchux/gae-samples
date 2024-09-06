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

import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.FutureCallback;

import io.github.bucket4j.Bucket;

import static java.time.Duration.ofSeconds;

public class DeleteIndexes extends HttpServlet {

  private static final ListeningExecutorService SERVICE = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(100));
  private static final AtomicLong COUNTER = new AtomicLong(0l);

  @Override
  public void doGet(
    HttpServletRequest request,
    HttpServletResponse response
  ) throws IOException {
    
    response.setContentType("text/plain");
    
    try {

      final Bucket bucket = Bucket.builder().withNanosecondPrecision()
        .addLimit(limit -> limit.capacity(300).refillIntervally(300, ofSeconds(1)))
        .build();

      final SearchService search = SearchServiceFactory.getSearchService();

      // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.SearchService
      final GetIndexesRequest getIndexesRequest = GetIndexesRequest.newBuilder().setAllNamespaces(true).build();
      final GetResponse<Index> getIndexesResponse = search.getIndexes(getIndexesRequest);

      final WaitGroup wg = new WaitGroup();
      for (final Index index : getIndexesResponse) {
        bucket.asBlocking().consume(1);

        final String indexNamespace = index.getNamespace();
        final String indexName = index.getName();
        final GetRequest getDocumentsRequest = GetRequest.newBuilder().setLimit(100).setReturningIdsOnly(true).build();
        
        while (true) {
          // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.Index
          final GetResponse<Document> getDocumentsResponse = index.getRange(getDocumentsRequest);
          final List<Document> documents = getDocumentsResponse.getResults();
          
          if (documents.size() == 0) {
            // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.Index#com_google_appengine_api_search_Index_deleteSchema__
            index.deleteSchema();
            break; // no more documents
          }
          
          wg.add();
          final ListenableFuture<Long> deletedDocuments = SERVICE.submit(new DeleteIndexedDocuments(index, bucket, documents));
          Futures.addCallback(deletedDocuments, new OnIndexedDocumentsDeleted(wg, indexNamespace, indexName), SERVICE);
        }
      
      }
      wg.wait();

      response.getWriter().println("OK: " + Long.toString(10l, 10));
    
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

  private static class OnIndexedDocumentsDeleted implements FutureCallback<Long> {

    private final WaitGroup wg;
    private final String namespace;
    private final String name;

    OnIndexedDocumentsDeleted(
      final WaitGroup wg,
      final String namespace,
      final String name
    ) {
      this.wg = wg;
      this.namespace = namespace;
      this.name = name;
    }

    public void onSuccess(Long deletedDocuments) {
      final long overallDeleterDocuments = COUNTER.addAndGet(deletedDocuments.longValue());
      System.out.println("Deleted Documents from index '" + this.namespace + "::" + this.name + "': " + Long.toString(overallDeleterDocuments, 10));
      this.wg.done();
    }

    public void onFailure(Throwable thrown) {
      thrown.printStackTrace(System.err);
      this.wg.done();
    }

  }

  private static class DeleteIndexedDocuments implements Function<Document, String>, Callable<Long> {

    private final Index index;
    private final Bucket bucket;
    private final List<Document> documents;

    DeleteIndexedDocuments(
      final Index index,
      final Bucket bucket,
      final List<Document> documents
    ) {
      this.index = index;
      this.bucket = bucket;
      this.documents = documents;
    }

    public Long call() {
      try {
        this.bucket.asBlocking().consume(1);
        final List<String> documentsIDs = Lists.transform(this.documents, this);
        // see: https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search.Index#com_google_appengine_api_search_Index_delete_java_lang_Iterable_java_lang_String__
        this.index.delete(documentsIDs);
        return Long.valueOf(documentsIDs.size());
      } catch(Exception e) {
        e.printStackTrace(System.err);
      }
      return Long.valueOf(0l);
    }

    public String apply(Document document) {
      return document.getId();
    }

  }

}
