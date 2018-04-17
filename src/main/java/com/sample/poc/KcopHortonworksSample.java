/* _______________________________________________________ {COPYRIGHT-TOP} _____
 * IBM Confidential
 * IBM InfoSphere Data Replication Source Materials
 *
 * 5725-E30 IBM InfoSphere Data Replication
 * 5725-E30 IBM InfoSphere Data Replication for Database Migration
 *
 * 5724-U70 IBM InfoSphere Change Data Delivery
 * 5724-U70 IBM InfoSphere Change Data Delivery for PureData System for Analytics
 * 5724-Q36 IBM InfoSphere Change Data Delivery for Information Server
 * 5724-Q36 IBM InfoSphere Change Data Delivery for PureData System for Analytics
 * for Information Server
 *
 * (C) Copyright IBM Corp. 2017  All Rights Reserved.
 *
 * The source code for this program is not published or otherwise
 * divested of its trade secrets, irrespective of what has been
 * deposited with the U.S. Copyright Office.
 * _______________________________________________________ {COPYRIGHT-END} _____*/

/****************************************************************************
** The following sample of source code ("Sample") is owned by International 
** Business Machines Corporation or one of its subsidiaries ("IBM") and is 
** copyrighted and licensed, not sold. You may use, copy, modify, and 
** distribute the Sample in any form without payment to IBM.
** 
** The Sample code is provided to you on an "AS IS" basis, without warranty of 
** any kind. IBM HEREBY EXPRESSLY DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR 
** IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
** MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do 
** not allow for the exclusion or limitation of implied warranties, so the above 
** limitations or exclusions may not apply to you. IBM shall not be liable for 
** any damages you suffer as a result of using, copying, modifying or 
** distributing the Sample, even if IBM has been advised of the possibility of 
** such damages.
*****************************************************************************/

package com.datamirror.ts.target.publication.userexit.sample.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import com.datamirror.ts.target.publication.userexit.ReplicationEventPublisherIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaCustomOperationProcessorIF;

/**
 * <BR> IMPORTANT!!!!!
 * <p> THIS CODE IS NOT COMPLETED OR TESTED, IT REPRESENTS A THEORETCIAL EXAMPLE OF HOW ONE
 * MIGHT ATTEMPT TO GO ABOUT INTEGRATING ANOTHER SCHEMA REGISTRY.  SEE ABOVE DISCLAIMER IN 
 * THIS FILE. </p>
 * 
 *  <p>This sample uses the Hortonworks schema registry. Because the Hortonworks serializer 
 *  requires a schema registry url and this code is responsible for serializing, schema.registry.url
 *  must set in a properties file and the name of that file must be passed into this user exit as a parameter.
 *  Additional Hortonworks serialization properties may also be set in the properties file, if desired.</p>
 *
 *  <p>To use this sample, the Hortonworks serializer and its dependencies must be placed on the classpath.
 *  Use Maven to get the Hortonworks serializer and its dependencies. To do so, create a pom.xml file with
 *  the following contents:
 *  <code>
 *  <br>
 *  &lt;project&gt;<br>
 *  &nbsp;&lt;modelVersion&gt;4.0.0&lt;/modelVersion&gt;<br>
 *  &nbsp;&lt;groupId&gt;hw&lt;/groupId&gt;<br>
 *  &nbsp;&lt;artifactId&gt;hw&lt;/artifactId&gt;<br>
 *  &nbsp;&lt;version&gt;1&lt;/version&gt;<br>
 *  &nbsp;&lt;dependencies&gt;<br>
 *  &nbsp;&nbsp;&lt;dependency&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;groupId&gt;com.hortonworks.registries&lt;/groupId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;artifactId&gt;schema-registry-serdes&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;version&gt;0.3.0&lt;/version&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;exclusions&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.slf4j&lt;/groupId&gt;<br> 
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;artifactId&gt;log4j-over-slf4j&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;/exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.apache.hadoop&lt;/groupId&gt;<br> 
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;artifactId&gt;hadoop-client&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;/exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.springframework&lt;/groupId&gt;<br> 
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;artifactId&gt;spring-context&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;/exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.hibernate&lt;/groupId&gt;<br> 
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;artifactId&gt;hibernate-validator&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;/exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;groupId&gt;com.github.fge&lt;/groupId&gt;<br> 
 *  &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&lt;artifactId&gt;json-schema-validator&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&nbsp;&lt;/exclusion&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;/exclusions&gt;<br>
 *  &nbsp;&nbsp;&lt;/dependency&gt;<br>
 *  &nbsp;&nbsp;&lt;dependency&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;groupId&gt;commons-codec&lt;/groupId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;artifactId&gt;commons-codec&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;version&gt;1.6&lt;/version&gt;<br>
 *  &nbsp;&nbsp;&lt;/dependency&gt;<br>
 *  &nbsp;&nbsp;&lt;dependency&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.apache.httpcomponents&lt;/groupId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;artifactId&gt;httpclient&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;version&gt;4.5.2&lt;/version&gt;<br>
 *  &nbsp;&nbsp;&lt;/dependency&gt;<br>
 *  &nbsp;&nbsp;&lt;dependency&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.apache.httpcomponents&lt;/groupId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;artifactId&gt;httpcore&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;version&gt;4.4.4&lt;/version&gt;<br>
 *  &nbsp;&nbsp;&lt;/dependency&gt;<br>
 *  &nbsp;&nbsp;&lt;dependency&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;groupId&gt;org.slf4j&lt;/groupId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;artifactId&gt;slf4j-api&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;version&gt;1.7.21&lt;/version&gt;<br>
 *  &nbsp;&nbsp;&lt;/dependency&gt;<br>
 *  &nbsp;&nbsp;&lt;dependency&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;groupId&gt;commons-io&lt;/groupId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;artifactId&gt;commons-io&lt;/artifactId&gt;<br>
 *  &nbsp;&nbsp;&nbsp;&lt;version&gt;2.5&lt;/version&gt;<br>
 *  &nbsp;&nbsp;&lt;/dependency&gt;<br>
 *  &nbsp;&lt;/dependencies&gt;<br>
 *  &lt;/project&gt;<br>
 *  </code>
 *  </p>
 *  
 *  <p>Then run Maven with the following command line:<br>
 *  <code>mvn org.apache.maven.plugins:maven-dependency-plugin:3.0.0:copy-dependencies -DincludeScope=runtime</code>
 *  </p>
 *  
 *  <p>This will put jar files into the target/dependency subdirectory.</p>
 *  
 *  <p>Create a file name system.cp in CDC's instance/&lt;instance&gt;/conf/ directory with the path to the Hortonworks
 *  dependencies (/&lt;path&gt;/target/dependency/*). The asterisk at the end means all jar files in that directory.</p>
 *  
 *  <p>Restart the CDC instance in order for the new classpath to take effect.</p>
 *  
 *  <p>NOTE 1:  The createProducerRecords class is not thread safe.  However a means is provided so that each thread
 *   can store its own copy of non-theadsafe objects.  Please see how this is done below.</p>
 *    
 *  <p>NOTE 2:  The records returned by createProducerRecords are not deep copied, so each call to the method should
 *   generate new records and not attempt to keep references to old ones for reuse.</p>
 *   
 *  <p>Note 3:  The KCOP is instantiated once per subscription which registers the KCOP.  This means if statics are made
 *   use of, they will potentially be shared across the instantiated KCOPs belonging to multiple actively replicating 
 *   subscriptions.</p>
 *  
 */
public class KcopHortonworksSample implements KafkaCustomOperationProcessorIF
{
   private final Map<String, Object> serializerProperties = new Hashtable<String, Object>();
   private static final String FILE_HEADER="-file:";

   @Override
   /**
    * The init method is run once for a given subscription when the mirroring or replication is started.
    * It is run before any calls to creatProducerRecors are made.  
    * 
    * 1) A user should subscribe their Kafka Custom Operation Processor (KCOP) to the events they wish to handle.
    *   Any un-subscribed events will produce no Kafka Records into user topics. Currently BEFORE_INSERT_EVENT,
    *   BEFORE_UPDATE_EVENT and BEFORE_DELETE_EVENT are available events for subscription.
    *   
    * 2) It is possible some parsing and handling of parameters may be useful here in more complex scenarios, because
    *   they would only be parsed and processed once and at the beginning of KCOP execution.
    * 
    * @param kafkaKcopCoordinator - In general the kafkaKcopCoordinator should be thought of as an object which offers access to things which have a 
    * replication session wide scope.  As per the above a user indicates to the KCOP framework which events they wish to
    * handle by calling subscribeEvent.
    */
   public void init(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {   
      // Subscribe to the available operations.
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_INSERT_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_DELETE_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_UPDATE_EVENT);  
      
      // Read Mapping Properties file to know which User Kafka Topic to write each operation to.
      Properties properties = loadKCOPConfigurationProperties(
         kafkaKcopCoordinator.getParameter(),
         kafkaKcopCoordinator);

      for (Map.Entry<Object, Object> property : properties.entrySet())
      {
         serializerProperties.put(property.getKey().toString(), property.getValue());
      }
      
      // An optional example of Logging a message in the event log to indicate the subscription has started and is
      // making use of a KCOP.
      kafkaKcopCoordinator.logEvent("The Kafka Custom Operation Processor, KcopHortonworksSample, will be used for operation formatting."); 
   }

   private Properties loadKCOPConfigurationProperties(String KCOPConfigPropertiesFile, KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      FileInputStream configFileStream = null;

      Properties serializerConfigProperties = new Properties();
      String kcopParameter = kafkaKcopCoordinator.getParameter();
      if (kcopParameter == null || kcopParameter.isEmpty())
      {
         kafkaKcopCoordinator.logEvent("No parameter was provided to the KcopHortonworksSample and one is required.  Stopping Replication.");
         throw new  UserExitException("No parameter was provided to the KcopHortonworksSample and one is required.  Stopping Replication.");
      }
      if (kcopParameter.startsWith(FILE_HEADER))
      {
         String fileName = kcopParameter.substring(FILE_HEADER.length());    
         try
         {
            configFileStream = new FileInputStream(fileName);
            serializerConfigProperties.load(configFileStream);
         }
         catch (FileNotFoundException e)
         {          
            kafkaKcopCoordinator.logEvent("A properties file for KcopHortonworksSample was not found yet one was specified to be used.  Stopping Replication.");
            throw new  UserExitException(e.getMessage());
         }
         catch (IOException e)
         {  
            kafkaKcopCoordinator.logEvent("An IOException was encountered when attempting to load the properties file provided by the user for KcopHortonworksSample.");
            throw new UserExitException(e.getMessage());
         }
         finally
         {
            if(configFileStream != null)
            {
               try
               {
                  configFileStream.close();
               }
               catch (IOException e)
               {
                  throw new UserExitException(e.getMessage());
               }
            }
         }
      }
      else
      {
         serializerConfigProperties.put("schema.registry.url", kcopParameter);
      }
      return serializerConfigProperties;
   }

   @Override
   /**
    * createProducerRecords is called once per subscribed operation that occurs on the CDC Source.  This method is called by multiple threads
    * and as such operations are not necessarily processed in order.  It is valid to return no producer records if a user wishes nothing to 
    * be written to User Kafka Topics in response to the operation being processed.
    * 
    * For a given replication session, Kafka records written to the same topic and same partition will be written to Kafka in order.
    * ie.  Although formatted out of order, for a given topic/partition combination the resultant records will be sent in the original order
    * for a replication session.
    * 
    * By virtue of being able to specify the producer records, a user can determine:
    * 
    * 1) the topic(s) written to in response to an operation.  You could potentially alter the default behavior to write to two different topics.
    * 2) The partition of a topic the kafka record is written to.  This user exit always writes to partition 0 of a topic as per the default behavior.
    * 3) The format and bytes of the Key and Value portions of the Kafka record.  eg. The user has control over serialization
    * 
    * In contrast to the kafkaKcopCoordinator, the kafkaKcopOperationIn holds information relevant to the specific operation being processed and can thus
    * be thought of as specific operation scope rather than subscription scope.
    *
    *  NOTE 1:  The createProducerRecords class is not thread safe.  However a means is provided so that each thread
    *   can store its own copy of non-theadsafe objects.  Please see how this is done below.
    *    
    *  NOTE 2:  The records returned by createProducerRecords are not deep copied, so each call to the method should
    *   generate new records and not attempt to keep references to old ones for reuse.
    *   
    * @param kafkaKcopOperationIn - Contains information relevant to the current operation being processed. 
    * @param kafkaKcopCoordinator
    * @return producer records
    */
   public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords( KafkaKcopOperationInIF kafkaKcopOperationIn,
      KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      // The list of records to be applied to user Kafka topics in response to the original source database operation.  
      // The order of operations in the list determines their relative order in being applied for a replication session, If
      // the records refer to the same topic/partition combination.
      ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<ProducerRecord<byte[], byte[]>>();
      
      // Retrieve the thread specific user defined object if previously set by the user.
      PersistentProducerObject kafkaKcopHelperObjects = (PersistentProducerObject) kafkaKcopCoordinator.getKcopThreadSpecificContext();

      if (kafkaKcopHelperObjects == null)
      {
         // As no thread specific user defined object existed for this thread, create one so we can place our non-threadsafe objects
         // in it.  We will now not need to re-instantiate objects on each call to createProducerRecords, rather we can reuse ones
         // stored here.
         kafkaKcopHelperObjects = createPersistentProducerObject(kafkaKcopOperationIn, kafkaKcopCoordinator);
         // Having created an object for this thread which itself contains useful objects, some of which may be non-threadsafe, 
         // store the object so we won't need to recreate the next time this thread calls createProducerRecords.
         kafkaKcopCoordinator.setKcopThreadSpecificContext(kafkaKcopHelperObjects);
      }
      
      //Process the various events we have subscribed to, generating appropriate Kafka ProducerRecords as a result.
      if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_INSERT_EVENT)
      {
         
         ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;
         insertKafkaAvroProducerRecord = createDefaultAvroBinaryInsertProducerRecord (kafkaKcopOperationIn, kafkaKcopHelperObjects);
         // An insert on the source database is represented by one resultant kafka ProducerRecord in the appropriate Kafka topic.
         producerRecordsToReturn.add(insertKafkaAvroProducerRecord);      
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_DELETE_EVENT)
      {
         ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord;
         deleteKafkaProducerRecord = createDefaultAvroBinaryDeleteProducerRecord (kafkaKcopOperationIn, kafkaKcopHelperObjects, kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord());
         // A delete on the source database is represented by one resultant kafka ProducerRecord in the appropriate Kafka topic.
         // The key of the delete indicates which row no longer exists, the value bytes of a delete's Kafka ProducerRecord is null.
         producerRecordsToReturn.add(deleteKafkaProducerRecord);
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_UPDATE_EVENT)
      {
         // Determine if this update altered one of the Kafka Key Columns
         if (kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord() != null)
         {
            // Because the key has changed we need to first delete the kafka record with the old key and then insert the new one.
            ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord;
            deleteKafkaProducerRecord = createDefaultAvroBinaryDeleteProducerRecord (kafkaKcopOperationIn, kafkaKcopHelperObjects, kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord());
            producerRecordsToReturn.add(deleteKafkaProducerRecord); 
           
         }
         
         // In both the key update case and the non-key update case an insert of the new record being updated to is required.
         ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;
         insertKafkaAvroProducerRecord = createDefaultAvroBinaryInsertProducerRecord (kafkaKcopOperationIn, kafkaKcopHelperObjects);
         producerRecordsToReturn.add(insertKafkaAvroProducerRecord);

         
      }     
      // Note that their are two records being returned in response to an update which affected a key column.  The delete is added
      // to the List first, followed by the insert so that this is the expected order they will be seen on the User Kafka topic.
      return producerRecordsToReturn;
   }
   
   /**
    * 
    * @param kafkaKcopOperationIn -  Contains information relevant to the current operation being processed.
    * @param kafkaKcopHelperObjects - Contains subscription scope information including storage for each thread's non-threadsafe objects, in this case our serializers.
    * @return A single Kafka Producer Record representing an insert on the source table to be written to a User Kafka Topic.
    */
   private ProducerRecord<byte[], byte[]> createDefaultAvroBinaryInsertProducerRecord (KafkaKcopOperationInIF kafkaKcopOperationIn, PersistentProducerObject kafkaKcopHelperObjects)
   {
      ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;
      
      // For the Insert generate the bytes to place in the ProducerRecord for the Key field.  This handles a case where the key is null, which is not something
      // expected for default Kafka Replication, but we'll add as this code for completeness.  Note that the serializer employed here is registering the Avro
      // Generic Data Record with the Hortonworks schema registry transparently.
      byte[] kafkaAvroKeyByteArray = kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord() == null ? new byte[0] : kafkaKcopHelperObjects.getHortonworksKeySerializer().serialize(
         kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord());
      
      // For the Insert generate the bytes to place in the ProducerRecord for the Value field.  Note that the serializer employed here is registering the Avro
      // Generic Data Record with the Hortonworks schema registry transparently.
      byte[] kafkaAvroValueByteArray = kafkaKcopHelperObjects.getHortonworksValueSerializer().serialize( kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaKcopOperationIn.getKafkaAvroValueGenericRecord());
      
      // Create a NEW ProducerRecord object which will ultimately be written to the Kafka topic and partition specified in the ProducerRecord.
      insertKafkaAvroProducerRecord = new ProducerRecord<byte[], byte[]> 
      (
         kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaKcopOperationIn.getPartition(),
         (kafkaAvroKeyByteArray.length != 0) ? kafkaAvroKeyByteArray : null,
         (kafkaAvroValueByteArray.length != 0) ? kafkaAvroValueByteArray : null          
      );
      
      return insertKafkaAvroProducerRecord;
   }
  /**
   * 
   * @param kafkaKcopOperationIn -  Contains information relevant to the current operation being processed.
   * @param kafkaKcopHelperObjects - Contains subscription scope information including storage for each thread's non-threadsafe objects, in this case our serializers.
   * @param kafkaAvroKeyGenericRecord - 
   * @return producer record
   */
   private ProducerRecord<byte[], byte[]> createDefaultAvroBinaryDeleteProducerRecord (KafkaKcopOperationInIF kafkaKcopOperationIn, PersistentProducerObject kafkaKcopHelperObjects, GenericRecord kafkaAvroKeyGenericRecord)
   {
      ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord;
      
      // For the Delete generate the bytes to place in the ProducerRecord for the Key field.
      // Note that the key being specified logically represents the "row" being deleted on the source table.
      // The null case is handled although the documented default Kafka behavior will not send one. If there is no Key a delete would delete nothing, so the record
      // to Kafka in that case would be essentially a no-op from the perspective of the consumer.
      // Note that the serializer employed here is registering the Avro Generic Data Record with the Hortonworks schema registry transparently.
      byte[] kafkaAvroKeyByteArray = kafkaAvroKeyGenericRecord == null ? new byte[0] : kafkaKcopHelperObjects.getHortonworksKeySerializer().serialize(
         kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaAvroKeyGenericRecord);
      
      // A delete in Kafka is indicated by specifying that the Value bytes are null.  Upon compaction, Kafka then interprets the key as indicating
      // that any former values associated with this key are to be deleted.  The user topic and partition specified should correlate to the pairing where
      // the record being deleted was written.
      deleteKafkaProducerRecord = new ProducerRecord<byte[], byte[]>
      (
         kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaKcopOperationIn.getPartition(),
         (kafkaAvroKeyByteArray.length != 0) ? kafkaAvroKeyByteArray : null,
         null          
      ); 
      
      return deleteKafkaProducerRecord;
   }
   
   /**
    * 
    * @param kafkaKcopOperationIn -  Contains information relevant to the current operation being processed.
    * @param kafkaKcopCoordinator - Contains subscription scope information including storage for each thread's non-threadsafe objects.
    * @return - An object containing non-threadsafe objects.  Each thread executing createProducerRecords will create its own copy of this object and will be able to
    *           reuse it on that thread's subsequent callsto createProducerRecords as it will be stored in the kafkaUECoordinator, one for each thread.
    */
   @SuppressWarnings("unchecked")
   private PersistentProducerObject createPersistentProducerObject ( KafkaKcopOperationInIF kafkaKcopOperationIn,  ReplicationEventPublisherIF kafkaKcopCoordinator)
   {
      Serializer<Object> hortonworksKeySerializer;
      Serializer<Object> hortonworksValueSerializer;
      
      // Create the two serializers this thread will make use of for each call to createProducerRecords.  Saving these serializer objecst avoids having to instantiate
      // each time.
      try
      {
         Class<?> serializerClass = Class.forName("com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer");
         hortonworksKeySerializer = (Serializer<Object>) serializerClass.newInstance();     
         hortonworksValueSerializer = (Serializer<Object>) serializerClass.newInstance();
      }
      catch (ClassNotFoundException e)
      {
         throw new AssertionError(e);
      }
      catch (IllegalAccessException e)
      {
         throw new AssertionError(e);
      }
      catch (InstantiationException e)
      {
         throw new AssertionError(e);
      }
      
      // Set the properties of the Key and Value serializer.
      hortonworksKeySerializer.configure(serializerProperties, true);
      hortonworksValueSerializer.configure(serializerProperties, false);
      
      // Create the persistent object to be stored for this thread.
      return (new PersistentProducerObject(hortonworksKeySerializer, hortonworksValueSerializer));
   }
   
   /**
    * 
    * A Convenient class which stores objects relevant to performing default behavior replication.  One of these objects will be created for each thread
    * processing source database events.   By virtue of persisting these objects we avoid having to instantiate on each call to createProducerRecords.
    * This class contains objects which are not thread-safe but since a copy will ultimeately exists for each thread, we can make use of the objects.
    *
    */
   private class PersistentProducerObject
   {
      final Serializer<Object> hortonworksKeySerializer;
      final Serializer<Object> hortonworksValueSerializer;
      
      
      public PersistentProducerObject(Serializer<Object> aHortonworksKeySerializer, Serializer<Object> aHortonworksValueSerializer)
      {
         hortonworksKeySerializer = aHortonworksKeySerializer;
         hortonworksValueSerializer = aHortonworksValueSerializer;
         
      }
  
      public Serializer<Object> getHortonworksKeySerializer ()
      {
         return hortonworksKeySerializer;
      }
      
      public Serializer<Object> getHortonworksValueSerializer ()
      {
         return hortonworksValueSerializer;
      }
   }

   @Override
   public void finish(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      // No need for any particular finish logic in this example.  An optional event log message could be generated if desired.
      
   }

}
