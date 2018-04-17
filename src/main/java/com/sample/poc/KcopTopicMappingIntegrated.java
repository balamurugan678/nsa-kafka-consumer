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

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaCustomOperationProcessorIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;

/**
 * 
 *  <p>This sample produces the same behavior as the default Kafka Replication code, ie. the behavior without the 
 *  Kafka Custom Operation Processor, KCOP, ( aka User Exit) with one exception.  It allows the user to 
 *  choose which topic a given source table's operations are to be written to at a source table 
 *  granularity.</p>
 *  
 *  <p>To Explicitly map a given source to a given topic name use MAP_TABLE_ as a prefix to the default topic name.
 *  The following file example would map all tables to a default topic named DefaultTopicTest and 
 *  kafka1.kafka_multi_tab.sourcedb.shawnrr.tab2 to a topic named remappedTab2.</p>
 *  
 *  <br>$ cat kcopTopicMapping.properties 
 *  <br>schema.registry.url=http://localhost:8081
 *  <br>MAP_DEFAULT=DefaultTopicTest
 *  <br>MAP_TABLE_kafka1.kafka_multi_tab.sourcedb.shawnrr.tab2=remappedTab2
 *  
 *  <p>To map all source tables to a single topic use MAP_ALL rather than using MAP_ DEFAULT with no explicit mappings.
 *   Both would technically work, but MAP_ALL avoids an internal lookup for every call to createProducerRecords.</p>
 *  
 *  <br>[nz@shawnrrvm conf]$ cat kcopTopicMapping.properties 
 *  <br>schema.registry.url=http://localhost:8081
 *  <br>MAP_ALL=OneTopicToRuleThemAll
 *  
 *  
 *  <p> When combining multiple records from different source tables to the same topic, it may be wise to include a source
 *  table column in the mapping so you'll know which table the record consumed corresponded to. </p>
 *  
 *  <p> In the same manner that topics are re-mapped, the KCOP could be modified so that a partition could be selected. </p>
 *  
 *  <p> The file parameter must be prefixed by -file: ... an example would be... -file:/home/nz/CDC_KAFKA/CDC/conf/AvroLiveAudit.properties </p>
 *  
 *  <p>This code makes use of the Confluent serializer, thus transparently registering schemas. Because the default
 *   Confluent serializer requires a schema registry url and this code is responsible for serializing, the schema
 *  registry url must be passed into this user exit as a parameter.</p>
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
public class KcopTopicMappingIntegrated implements KafkaCustomOperationProcessorIF
{

   private static final int CONFLUENT_SCHEMA_REGISTRY_CACHE_SIZE = 5000;
   // mapAllTopic represents an override for the most common situation where all source table operations
   // are to be mapped to one topic. If mapAllTopic is chosen all other mapping properties are ignored.
   private String mapAllTopic; 
   // The default topic is used if wishing to explicitly specify some topics and default the remainder
   // to a topic.
   private String defaultTopic;
   // Prefix for mapping a specific source table to a chosen topic.
   private static final String specificRemapPrefix = "MAP_TABLE_";
   private Properties kCOPConfigurationProperties;
   private String schemaRegistryURL;
   
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
      kCOPConfigurationProperties = loadKCOPConfigurationProperties(kafkaKcopCoordinator.getParameter(), kafkaKcopCoordinator);
      processMappingProperties (kCOPConfigurationProperties);
      
     
      // Log an event indicating the Kcop to be used for formatting coordinator logging function.
      kafkaKcopCoordinator.logEvent("The Kafka Custom Operation Processor, " + this.getClass().getSimpleName() + " will be used for operation formatting.");
   }


   /**
    * Reads properties from the file path provided.  Throws an error if the file path is not properly prefixed or the indicated file cannot be read.
    * @param KCOPConfigPropertiesFile - an absolute file path prefixed with "-file:"  The file contains the properties for the mapping.
    * @param kafkaKcopCoordinator - In general the kafkaKcopCoordinator should be thought of as an object which offers access to things which have a 
    * replication session wide scope.
    * @return
    * @throws UserExitException
    */
   private Properties loadKCOPConfigurationProperties(String KCOPConfigPropertiesFile, KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      FileInputStream configFileStream = null;
      String FILE_HEADER="-file:";
      Properties kafkaKcopConfigProperties = new Properties();
      
      if (    KCOPConfigPropertiesFile == null
          || !KCOPConfigPropertiesFile.startsWith(FILE_HEADER))
      {
         kafkaKcopCoordinator.logEvent("A file is required as a parameter.  The parameter must start with the prefix: " + FILE_HEADER + "The following was the parameter provided: "
                                       + KCOPConfigPropertiesFile);
         throw new UserExitException("A file is required as a parameter.  The parameter must start with the prefix: " + FILE_HEADER + "The following was the parameter provided: "
                                      + KCOPConfigPropertiesFile);
      }
      else
      {
         String filename = KCOPConfigPropertiesFile.substring(FILE_HEADER.length());
         try
         {
            configFileStream = new FileInputStream(filename);
            kafkaKcopConfigProperties.load(configFileStream);
         }
         catch (FileNotFoundException e)
         {          
            kafkaKcopCoordinator.logEvent("A properties file for " + this.getClass().getSimpleName() + " was not found. Default Source Table to Kafka topic mapping will be used.");      
         }
         catch (IOException e)
         {  
            kafkaKcopCoordinator.logEvent("An IOException was encountered when attempting to load the properties file provided by the user for " + this.getClass().getSimpleName());
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
      
      return kafkaKcopConfigProperties;
   }

   /**
    * Parses the mapping properties provided in the KCOP properties file.
    * @param kCOPConfigurationProperties - Properties read from the input properties file.
    * @throws UserExitException - thrown if no schema registry url is provided.  One is required for the deserializer.
    */
   private void processMappingProperties(Properties kCOPConfigurationProperties) throws UserExitException
   {      
      // The URL for the schema registry is provided in the properties file, it must always be obtained.
      setSchemaRegistryURL(kCOPConfigurationProperties.getProperty("schema.registry.url"));
      if (getSchemaRegistryURL() == null)
      {
         throw new UserExitException("In the properties file provided via parameter to the " + this.getClass().getSimpleName() + ", schema.registry.url must be specified.");
      }
      
      
      // Determine if the option to map all topics to a single topic was specified.
      //setMapAllTopic(kCOPConfigurationProperties.getProperty("MAP_ALL"));
      if (kCOPConfigurationProperties.getProperty("MAP_ALL") != null)
      {
         setMapAllTopic(kCOPConfigurationProperties.getProperty("MAP_ALL"));
         return;
      }
      
      // Determine if a default topic to map to was specified for topics not listed in the properties file.
      // If this property is not set, then by default topics will map to the one the CDC Kafka Target would normally choose.
      // If this property is set, but no individual topic mappings are specified, then its effect will be similar to having
      // specified the MAP_ALL property, but less efficient.
      if (kCOPConfigurationProperties.getProperty("MAP_DEFAULT") != null)
      {
         setDefaultTopic(kCOPConfigurationProperties.getProperty("MAP_DEFAULT"));
         kCOPConfigurationProperties.remove("MAP_DEFAULT");
      }  
   }
   
   /**
    * 
    * @param providedURL - sets the schema registry url.
    */
   private void setSchemaRegistryURL(String providedURL)
   {
      schemaRegistryURL = providedURL;
      
   }
   
   /**
    * 
    * @return - returns the schema registry url.
    */
   private String getSchemaRegistryURL()
   {
      return schemaRegistryURL;
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
    * @param kafkaKcopCoordinator - Contains subscription level information and a means of storing objects.
    * @return - zero or more Kafka producer recrods.
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
         insertKafkaAvroProducerRecord = createReMappedAvroBinaryInsertProducerRecord (kafkaKcopOperationIn, kafkaKcopHelperObjects, kafkaKcopCoordinator);
         // An insert on the source database is represented by one resultant kafka ProducerRecord in the appropriate Kafka topic.
         producerRecordsToReturn.add(insertKafkaAvroProducerRecord);      
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_DELETE_EVENT)
      {
         ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord;
         deleteKafkaProducerRecord = createRemappedAvroBinaryDeleteProducerRecord (kafkaKcopOperationIn, 
                                                                                  kafkaKcopHelperObjects, 
                                                                                  kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord(),
                                                                                  kafkaKcopCoordinator);
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
            deleteKafkaProducerRecord = createRemappedAvroBinaryDeleteProducerRecord (kafkaKcopOperationIn, 
                                                                                     kafkaKcopHelperObjects, 
                                                                                     kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord(),
                                                                                     kafkaKcopCoordinator);
            producerRecordsToReturn.add(deleteKafkaProducerRecord); 
           
         }
         
         // In both the key update case and the non-key update case an insert of the new record being updated to is required.
         ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;
         insertKafkaAvroProducerRecord = createReMappedAvroBinaryInsertProducerRecord (kafkaKcopOperationIn, kafkaKcopHelperObjects, kafkaKcopCoordinator);
         producerRecordsToReturn.add(insertKafkaAvroProducerRecord);

         
      }     
      // Note that their are two records being returned in response to an update which affected a key column.  The delete is added
      // to the List first, followed by the insert so that this is the expected order they will be seen on the User Kafka topic.
      return producerRecordsToReturn;
   }
   
   /**
    * 
    * @param kafkaKcopOperationIn -  Contains information relevant to the current operation being processed.
    * @param kafkaKcopHelperObjects - Contains an object for each thread which can contian non-threadsafe objects, in this case our serializers.
    * @param kafkaKcopCoordinator - Contains subscription scope information including storage for each thread's non-threadsafe objects.
    * @return A single Kafka Producer Record representing an insert on the source table to be written to a User Kafka Topic.
    */
   private ProducerRecord<byte[], byte[]> createReMappedAvroBinaryInsertProducerRecord (KafkaKcopOperationInIF kafkaKcopOperationIn, 
                                                                                        PersistentProducerObject kafkaKcopHelperObjects, 
                                                                                        KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

      
      StringBuilder kafkaTopicName = new StringBuilder();
      if (getMapAllTopic() != null)
      {
         kafkaTopicName.append(getMapAllTopic());
      }
      else
      {
         StringBuilder specificTableMapPropertyName = new StringBuilder ();
         specificTableMapPropertyName.append (specificRemapPrefix);
         // <CDC Target Instance>.<Subscription Name>.<Source Database>.<Source Schema>.<Table Name>
         specificTableMapPropertyName.append (kafkaKcopOperationIn.getKafkaTopicName());
         String specificTableMapping = kCOPConfigurationProperties.getProperty(specificTableMapPropertyName.toString());
         if (specificTableMapping != null)
         {
            kafkaTopicName.append(specificTableMapping);
         }
         else
         {
            if(getDefaultTopic() != null)
            {
               
               kafkaTopicName.append(getDefaultTopic());
            }
            else
            {
               kafkaTopicName.append(kafkaKcopOperationIn.getKafkaTopicName());
            }
         }
      }
      
      
      // For the Insert generate the bytes to place in the ProducerRecord for the Key field.  This handles a case where the key is null, which is not something
      // expected for default Kafka Replication, but we'll add as this code for completeness.  Note that the serializer employed here is registering the Avro
      // Generic Data Record with the Confluent schema registry transparently.
      byte[] kafkaAvroKeyByteArray = kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord() == null ? new byte[0] : kafkaKcopHelperObjects.confluentKeySerializer.serialize(
         kafkaTopicName.toString(),
         kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord());
      
      // For the Insert generate the bytes to place in the ProducerRecord for the Value field.  Note that the serializer employed here is registering the Avro
      // Generic Data Record with the Confluent schema registry transparently.
      byte[] kafkaAvroValueByteArray = kafkaKcopHelperObjects.getConfluentValueSerializer().serialize( kafkaTopicName.toString(),
         kafkaKcopOperationIn.getKafkaAvroValueGenericRecord());
      
      // Create a NEW ProducerRecord object which will ultimately be written to the Kafka topic and partition specified in the ProducerRecord.
      insertKafkaAvroProducerRecord = new ProducerRecord<byte[], byte[]> 
      (
         kafkaTopicName.toString(),
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
   private ProducerRecord<byte[], byte[]> createRemappedAvroBinaryDeleteProducerRecord (KafkaKcopOperationInIF kafkaKcopOperationIn, 
                                                                                       PersistentProducerObject kafkaKcopHelperObjects, 
                                                                                       GenericRecord kafkaAvroKeyGenericRecord,
                                                                                       KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      ProducerRecord<byte[], byte[]> deleteKafkaProducerRecord;
      
     
      StringBuilder kafkaTopicName = new StringBuilder ();
      if (getMapAllTopic() != null)
      {
         kafkaTopicName.append(getMapAllTopic());
      }
      else
      {
         StringBuilder specificTableMapPropertyName = new StringBuilder ();
         specificTableMapPropertyName.append (specificRemapPrefix);
         // <CDC Target Instance>.<Subscription Name>.<Source Database>.<Source Schema>.<Table Name>
         specificTableMapPropertyName.append (kafkaKcopOperationIn.getKafkaTopicName());
         String specificTableMapping = kCOPConfigurationProperties.getProperty(specificTableMapPropertyName.toString());
         if (specificTableMapping != null)
         {
            kafkaTopicName.append(specificTableMapping);
         }
         else
         {
            if(getDefaultTopic() != null)
            {
               
               kafkaTopicName.append(getDefaultTopic());
            }
            else
            {
               kafkaTopicName.append(kafkaKcopOperationIn.getKafkaTopicName());
            }
         }
      }
      
      // For the Delete generate the bytes to place in the ProducerRecord for the Key field.
      // Note that the key being specified logically represents the "row" being deleted on the source table.
      // The null case is handled although the documented default Kafka behavior will not send one. If there is no Key a delete would delete nothing, so the record
      // to Kafka in that case would be essentially a no-op from the perspective of the consumer.
      // Note that the serializer employed here is registering the Avro Generic Data Record with the Confluent schema registry transparently.
      byte[] kafkaAvroKeyByteArray = kafkaAvroKeyGenericRecord == null ? new byte[0] : kafkaKcopHelperObjects.confluentKeySerializer.serialize(
         kafkaTopicName.toString(),
         kafkaAvroKeyGenericRecord);
      
      
      // A delete in Kafka is indicated by specifying that the Value bytes are null.  Upon compaction, Kafka then interprets the key as indicating
      // that any former values associated with this key are to be deleted.  The user topic and partition specified should correlate to the pairing where
      // the record being deleted was written.
      deleteKafkaProducerRecord = new ProducerRecord<byte[], byte[]>
      (
         kafkaTopicName.toString(),
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
   private PersistentProducerObject createPersistentProducerObject ( KafkaKcopOperationInIF kafkaKcopOperationIn,  KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      KafkaAvroSerializer confluentKeySerializer;
      KafkaAvroSerializer confluentValueSerializer;
      Map <String, Object> propertiesMap = new Hashtable<String, Object>();
      
      // The parameter provided through the Kafka Custom Operation Processor, KCOP, interface.  In this example the parameter provided is the Schema Registry URL
      // which is used when instantiating the Confluent Avro Binary Serializers so that they can transparently contact.  Note in other scenarios this parameter could
      // represent the file location of a property list and processed in the init function.
      String baseSchemaRegistryUrl = getSchemaRegistryURL();
      
      // Create the two serializers this thread will make use of for each call to createProducerRecords.  Saving these serializer objecst avoids having to instantiate
      // each time.
      confluentKeySerializer = new KafkaAvroSerializer ();     
      confluentValueSerializer = new KafkaAvroSerializer ();
      
      // Format the URL to make it suitable for the property that the Confluent serializer expects to be provided with.
      if (baseSchemaRegistryUrl.endsWith("/"))
      {
         propertiesMap.put("schema.registry.url", baseSchemaRegistryUrl.substring(0, baseSchemaRegistryUrl.length() - 1));
      }
      else
      {
         propertiesMap.put("schema.registry.url", baseSchemaRegistryUrl);
      }
      // Define a cache size for the instantiated Confluent serializer to save known schemas in.
      propertiesMap.put("max.schemas.per.subject", CONFLUENT_SCHEMA_REGISTRY_CACHE_SIZE);
      
      // Set the properties of the Key and Value serializer.
      confluentKeySerializer.configure(propertiesMap, true);
      confluentValueSerializer.configure(propertiesMap, false);
      
      // Create the persistent object to be stored for this thread.
      return (new PersistentProducerObject(confluentKeySerializer, confluentValueSerializer, baseSchemaRegistryUrl));
   }
   
   /**
    * 
    * A Convenient class which stores objects relevant to performing default behavior replication.  One of these objects will be created for each thread
    * processing source database events.   By virtue of persisting these objects we avoid having to instantiate on each call to createProducerRecords.
    * This class contains objects which are not thread-safe but since a copy will ultimeately exists for each thread, we can make use of the objects.
    *
    */
   static class PersistentProducerObject
   {
      final KafkaAvroSerializer confluentKeySerializer;
      final KafkaAvroSerializer confluentValueSerializer;
      String baseSchemaRegistryUrl;
      
      
      public PersistentProducerObject(KafkaAvroSerializer aConfluentKeySerializer, KafkaAvroSerializer aConfluentValueSerializer, String aBaseSchemaRegistryUrl)
      {
         confluentKeySerializer = aConfluentKeySerializer;
         confluentValueSerializer = aConfluentValueSerializer;
         baseSchemaRegistryUrl = aBaseSchemaRegistryUrl;
         
      }
  
      public KafkaAvroSerializer getConfluentKeySerializer ()
      {
         return confluentKeySerializer;
      }
      
      public KafkaAvroSerializer getConfluentValueSerializer ()
      {
         return confluentValueSerializer;
      }

      public String getBaseSchemaRegistryUrl ()
      {
         return baseSchemaRegistryUrl;
      }
      
   }

   @Override
   public void finish(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      // No need for any particular finish logic in this example.  An optional event log message could be generated if desired.
      
   }


   /**
    * Gets the topic which all topics are to be mapped to.
    * @return
    */
   public String getMapAllTopic()
   {
      return mapAllTopic;
   }


   /**
    * Sets the topic which all topics are to be mapped to.
    * @param mapAllTopicIn - the Topic which all topics are to be mapped to.
    */
   public void setMapAllTopic(String mapAllTopicIn)
   {
      this.mapAllTopic = mapAllTopicIn;
   }


   /**
    * Gets the topic which all topics not explicitly mapped, are mapped to.
    * @return  The Default topic to map to.
    */
   public String getDefaultTopic()
   {
      return defaultTopic;
   }


   /**
    * Sets the default topic to map to if a given topic is not explicitly mapped.
    * @param defaultTopic - the default topic to map to.
    */
   public void setDefaultTopic(String defaultTopic)
   {
      this.defaultTopic = defaultTopic;
   }

}
