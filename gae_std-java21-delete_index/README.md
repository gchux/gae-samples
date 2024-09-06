Forked from:

https://github.com/GoogleCloudPlatform/java-docs-samples/tree/main/appengine-java11/helloworld-servlet

---

## Install Google Cloud SDK

https://cloud.google.com/sdk/docs/install

## Deploy GAE Standard Service

```sh
./mvnw clean package appengine:deploy
```

## Delete ALL indexes

```sh
curl -ivL -XGET https://<gae-service>-dot-<gcp-project>.appspot.com/deleteIndexes?qps=300
```

> see [handler implementation](src/main/java/com/example/appengine/DeleteIndexes.java)

---

## REFERENCES

- https://cloud.google.com/appengine/docs/standard/java-gen2/runtime
- https://cloud.google.com/sdk/gcloud/reference/app/deploy
- https://cloud.google.com/appengine/docs/standard/java/maven-reference
- https://cloud.google.com/appengine/docs/standard/java-gen2/reference/services/bundled/latest/com.google.appengine.api.search
