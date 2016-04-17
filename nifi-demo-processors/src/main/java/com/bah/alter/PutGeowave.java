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
package com.bah.alter;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.store.DataStore;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.IndexWriter;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayOutputStream;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.json.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Tags({"geowave,accumulo,put"})
@CapabilityDescription("Writes data to Geowave")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute = "", description = "")})
@WritesAttributes({@WritesAttribute(attribute = "", description = "")})
public class PutGeowave extends AbstractProcessor {

  public static final PropertyDescriptor ZOOKEEPERS = new PropertyDescriptor
      .Builder().name("Zookeepers").description("List of zookeeper servers")
      .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  public static final PropertyDescriptor ACCUMULO_INSTANCE = new PropertyDescriptor
      .Builder().name("Accumulo Instance").description("Accumulo instance name")
      .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  public static final PropertyDescriptor ACCUMULO_USERNAME = new PropertyDescriptor
      .Builder().name("Accumulo Username").description("Accumulo username")
      .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  public static final PropertyDescriptor ACCUMULO_PASSWORD = new PropertyDescriptor
      .Builder().name("Accumulo Password").description("Accumulo Password")
      .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  public static final PropertyDescriptor GEOWAVE_NAMESPACE = new PropertyDescriptor
      .Builder().name("Geowave Namespace").description("Namespace of Geowave table to write to")
      .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

  public static final Relationship SUCCESS = new Relationship.Builder()
      .name("success")
      .description("Successful write")
      .build();

  public static final Relationship FAILURE = new Relationship.Builder()
      .name("failure")
      .description("Failed to write")
      .build();

  private List<PropertyDescriptor> descriptors;

  private Set<Relationship> relationships;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(ZOOKEEPERS);
    descriptors.add(ACCUMULO_INSTANCE);
    descriptors.add(ACCUMULO_USERNAME);
    descriptors.add(ACCUMULO_PASSWORD);
    descriptors.add(GEOWAVE_NAMESPACE);
    this.descriptors = Collections.unmodifiableList(descriptors);

    final Set<Relationship> relationships = new HashSet<Relationship>();
    relationships.add(SUCCESS);
    relationships.add(FAILURE);
    this.relationships = Collections.unmodifiableSet(relationships);

  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return descriptors;
  }

  Index index;
  SimpleFeatureBuilder pointBuilder;
  DataStore geowaveDataStore;
  FeatureDataAdapter adapter;
  SimpleDateFormat sf;

  @OnScheduled
  public void onScheduled(final ProcessContext context) {
    System.out.println("PutGeowave processor started");
    System.out.println(context.getProperties());

    BasicAccumuloOperations bao = null;
    try {
      bao = new BasicAccumuloOperations(context.getProperty("Zookeepers").toString(), context.getProperty("Accumulo Instance").toString(),
          context.getProperty("Accumulo Username").toString(), context.getProperty("Accumulo Password").toString(),
          context.getProperty("Geowave Namespace").toString());
    } catch (AccumuloException | AccumuloSecurityException e) {
      e.printStackTrace();
    }
    final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
    final AttributeTypeBuilder ab = new AttributeTypeBuilder();
    builder.setName("Point");
    builder.add(ab.binding(Geometry.class).nillable(false).buildDescriptor("geometry"));
    builder.add(ab.binding(Date.class).nillable(true).buildDescriptor("TimeStamp"));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor("Latitude"));
    builder.add(ab.binding(Double.class).nillable(false).buildDescriptor("Longitude"));
    builder.add(ab.binding(String.class).nillable(true).buildDescriptor("Comment"));

    geowaveDataStore = new AccumuloDataStore(new AccumuloIndexStore(bao), new AccumuloAdapterStore(bao), new AccumuloDataStatisticsStore(bao), bao);
    final SimpleFeatureType point = builder.buildFeatureType();
    pointBuilder = new SimpleFeatureBuilder(point);
    adapter = new FeatureDataAdapter(point);
    index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
    sf = new SimpleDateFormat("EEE MMM dd HH:mm:ss ZZZZZ yyyy", Locale.ENGLISH);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }

    System.out.println("PutGeowave processor executing ");
    System.out.println(context.getProperties());
    System.out.println(flowFile.toString());
    System.out.println(flowFile.getAttributes().toString());
    System.out.println(flowFile);
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    session.exportTo(flowFile, outputStream);
    System.out.println(outputStream.toString());
    JSONObject jsonObject = new JSONObject(outputStream.toString());
    if (jsonObject.has("coordinates")) {
      try{
        JSONObject coordinate = jsonObject.getJSONObject("coordinates");
        System.out.println("has coordinate");

        double longitude = 0;
        double latitude = 0;
        try {
          JSONArray point = coordinate.getJSONArray("coordinates");
          longitude = point.getDouble(0);
          latitude = point.getDouble(1);
        } catch (JSONException e) {
          System.out.println("Couldn't get coordinates");
          System.out.println(e.toString());
        }

        System.out.println("lat: " + latitude + " long: " + longitude);
        String featureId = "0";
        try {
          featureId = jsonObject.getString("id_str");
        } catch (JSONException e) {
          System.out.println("failed to get id");
          System.out.println(e.toString());
        }
        Date date = new Date();
        try {
          date = sf.parse(jsonObject.getString("created_at"));
        } catch (ParseException e) {
          System.out.println("Couldn't parse date");
          System.out.println(e.toString());
        }
        System.out.println("Date: "+date.toString());
        try (IndexWriter indexWriter = geowaveDataStore.createIndexWriter(index)) {
          pointBuilder.set("geometry", GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(longitude, latitude)));
          pointBuilder.set("TimeStamp", date);
          pointBuilder.set("Latitude", latitude);
          pointBuilder.set("Longitude", longitude);
          final SimpleFeature sft = pointBuilder.buildFeature(featureId);
          indexWriter.write(adapter, sft);
          System.out.println("wrote to accumulo");
          session.transfer(flowFile, SUCCESS);
        } catch (final IOException | JSONException e) {
          System.out.println("Failed to write to accumulo");
          session.transfer(flowFile, FAILURE);
          System.out.println(e.toString());
        }
      } catch(JSONException e) {
        System.out.println("No coordinates");
        session.transfer(flowFile, FAILURE);
      }
    }
  }
}
