/*
 * _______________________________________________________ {COPYRIGHT-TOP} _____ IBM Confidential IBM
 * InfoSphere Data Replication Source Materials
 * 
 * 5725-E30 IBM InfoSphere Data Replication 5725-E30 IBM InfoSphere Data Replication for Database Migration
 * 
 * 5724-U70 IBM InfoSphere Change Data Delivery 5724-U70 IBM InfoSphere Change Data Delivery for PureData
 * System for Analytics 5724-Q36 IBM InfoSphere Change Data Delivery for Information Server 5724-Q36 IBM
 * InfoSphere Change Data Delivery for PureData System for Analytics for Information Server
 * 
 * (C) Copyright IBM Corp. 2017 All Rights Reserved.
 * 
 * The source code for this program is not published or otherwise divested of its trade secrets, irrespective
 * of what has been deposited with the U.S. Copyright Office.
 * _______________________________________________________ {COPYRIGHT-END} _____
 */

/****************************************************************************
 ** The following sample of source code ("Sample") is owned by International Business Machines Corporation or
 * one of its subsidiaries ("IBM") and is copyrighted and licensed, not sold. You may use, copy, modify, and
 * distribute the Sample in any form without payment to IBM.
 ** 
 ** The Sample code is provided to you on an "AS IS" basis, without warranty of any kind. IBM HEREBY EXPRESSLY
 * DISCLAIMS ALL WARRANTIES, EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. Some jurisdictions do not allow for the exclusion
 * or limitation of implied warranties, so the above limitations or exclusions may not apply to you. IBM shall
 * not be liable for any damages you suffer as a result of using, copying, modifying or distributing the
 * Sample, even if IBM has been advised of the possibility of such damages.
 *****************************************************************************/

package com.datamirror.ts.target.publication.userexit.sample.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.datamirror.ts.target.publication.userexit.JournalHeaderIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventPublisherIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaCustomOperationProcessorIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;

/**
 *  <p>This sample produces an audit record in avro format. It uses journal control fields available in
 *  the Kcop API and uses an avro serializer to produce a generic avro record.Please see the examples
 *  below for how an insert, update and delete will appear below.</p>
 *  
 * Example Insert record of "1, 'abc'" into the source database:
 * <br>{"PKCOL":1,"STR1":{"string":"abc       "},"A_ENTTYP":"PT","A_TIMSTAMP":"2017-09-28 11:36:22.000000000000","A_USER":"SOME_USER","A_JOBUSER":"="}
 * <br>Example Delete of the inserted record above:
 * <br>{"PKCOL":1,"STR1":{"string":"abc       "},"A_ENTTYP":"DL","A_TIMSTAMP":"2017-09-28 11:36:51.000000000000","A_USER":"SOME_USER","A_JOBUSER":"="}
 * <br>Example Update of a record, where the row "1,'abc'" had 'abc' updated to 'def'. This produces two records, for before update and after update:
 * <br>{"PKCOL":1,"STR1":{"string":"abc       "},"A_ENTTYP":"UB","A_TIMSTAMP":"2017-09-28 11:36:51.000000000000","A_USER":"REGRUSR1","A_JOBUSER":"="}
 * <br>{"PKCOL":1,"STR1":{"string":"def       "},"A_ENTTYP":"DL","A_TIMSTAMP":"2017-09-28 11:36:51.000000000000","A_USER":"REGRUSR1","A_JOBUSER":"="}
 * <p>
 *  The input parameter can be the schema registry url, or a properties file. The properties file
 *  can be specified by using -file:[properties_file_path] in the user exit parameter.</p>
 *  
 *  <p>The properties file can define the following parameters:</p>
 *  
 *  schema.registry - the schema registry url
 *  <br>audit.jcfs - a comma-separated list of journal control field names to be added to each record
 * 
 *  <p>By default, the journal control fields that will be used are ENTTYP, TIMSTAMP, JOB, JOBUSER. See {@link JournalControlField}
 *  for the list of available journal control fields. To specify ENTTYP and CCID, create a properties file containing:</p>
 *  <p>
 *  schema.registry=https://[schema_registry_url]:[schema_registry_port]
 *  <br>audit.jcfs=ENTTYP,CCID </p>
 *   <p>NOTE 1:  The createProducerRecords class is not thread safe.  However a means is provided so that each thread
 *   can store its own copy of non-theadsafe objects.  Please see how this is done below.</p>
 *    
 *  <p>NOTE 2:  The records returned by createProducerRecords are not deep copied, so each call to the method should
 *   generate new records and not attempt to keep references to old ones for reuse.</p>
 *   
 *  <p>Note 3:  The KCOP is instantiated once per subscription which registers the KCOP.  This means if statics are made
 *   use of, they will potentially be shared across the instantiated KCOPs belonging to multiple actively replicating 
 *   subscriptions.</p>
 */
public class KcopMultiRowAvroLiveAuditIntegrated implements KafkaCustomOperationProcessorIF
{

   public static final int CONFLUENT_SCHEMA_REGISTRY_CACHE_SIZE = 5000;

   public static final String DEFAULT_AUDIT_COLUMNS = "ENTTYP,TIMSTAMP,USER,JOBUSER";

   private Properties kCOPConfigurationProperties;
   private String schemaRegistryURL;
   List<AvroJournalControlField> avroJournalControlFields;
  
   @Override
   public void init(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      // Subscribe to some operations
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_INSERT_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_DELETE_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_UPDATE_EVENT);

      kCOPConfigurationProperties = loadKCOPConfigurationProperties(
         kafkaKcopCoordinator.getParameter(),
         kafkaKcopCoordinator);

      processMappingProperties(kCOPConfigurationProperties, kafkaKcopCoordinator);
   }

   @Override
   public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords(
      KafkaKcopOperationInIF kafkaKcopOperationIn,
      KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<ProducerRecord<byte[], byte[]>>();

      PersistentProducerObject kafkaKcopHelperObjects = (PersistentProducerObject) kafkaKcopCoordinator
         .getKcopThreadSpecificContext();

      if (kafkaKcopHelperObjects == null)
      {
         kafkaKcopHelperObjects = createPersistentProducerObject(kafkaKcopOperationIn, kafkaKcopCoordinator);
         kafkaKcopCoordinator.setKcopThreadSpecificContext(kafkaKcopHelperObjects);
      }
      
      // Select the schema for this operation's source table mapping if it has already been generated.
      Schema liveAuditAvroValueSchema = kafkaKcopHelperObjects.topicToSchemaMap.get(kafkaKcopOperationIn.getKafkaTopicName());
      if (liveAuditAvroValueSchema == null)
      {
         // As this thread has not yet stored a schema for this table, generate and save one in the thread's specific context persistent object.
         // The first operation may either be an update or insert, having a value generic record OR a delete having a before value generic record.
         Schema avroBaseSchema = kafkaKcopOperationIn.getKafkaAvroValueGenericRecord() != null ? 
                                                                                                kafkaKcopOperationIn.getKafkaAvroValueGenericRecord().getSchema() : 
                                                                                                kafkaKcopOperationIn.getKafkaAvroBeforeValueGenericRecord().getSchema();
         liveAuditAvroValueSchema = generateKafkaAuditValueSchema(avroBaseSchema, kafkaKcopCoordinator);
         kafkaKcopHelperObjects.topicToSchemaMap.put(kafkaKcopOperationIn.getKafkaTopicName(), liveAuditAvroValueSchema);
      }

      JournalHeaderIF journalHeader = kafkaKcopOperationIn.getUserExitJournalHeader();
      JournalHeaderIF journalHeaderBefore = kafkaKcopOperationIn.getUserExitBeforeJournalHeader();

      // Create the avro audit record for insert, update and delete events.
      if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_INSERT_EVENT)
      {
         GenericRecord kafkaAvroValueGenericRecord = kafkaKcopOperationIn.getKafkaAvroValueGenericRecord();
         
         // Using the Audit Value Schema, generate a specific Avro record by combining the values contained in the input value generic record and the journal header.
         GenericRecord kafkaAvroValueAuditRecord = createKafkaAuditAvroRecord(
            kafkaAvroValueGenericRecord,
            journalHeader,
            liveAuditAvroValueSchema);

         ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

         // Use the serialized bytes of the Audit formatted Avro Generic record create a Kafka produce record.
         insertKafkaAvroProducerRecord = createKafkaAuditAvroBinaryProducerRecord(
            kafkaAvroValueAuditRecord,
            kafkaKcopOperationIn,
            kafkaKcopHelperObjects);

         producerRecordsToReturn.add(insertKafkaAvroProducerRecord);
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_DELETE_EVENT)
      {
         GenericRecord kafkaAvroBeforeValueGenericRecord = kafkaKcopOperationIn
            .getKafkaAvroBeforeValueGenericRecord();
         
         // For the audit of delete there will be the source table values which are deleted, plus JCFs, reflected in the Values field of the Kafka Producer Record.
         GenericRecord kafkaAvroValueAuditBeforeRecord = createKafkaAuditAvroRecord(
            kafkaAvroBeforeValueGenericRecord,
            journalHeaderBefore,
            liveAuditAvroValueSchema);

         ProducerRecord<byte[], byte[]> insertKafkaAvroBeforeProducerRecord;

         insertKafkaAvroBeforeProducerRecord = createKafkaAuditAvroBinaryProducerRecord(
            kafkaAvroValueAuditBeforeRecord,
            kafkaKcopOperationIn,
            kafkaKcopHelperObjects);

         producerRecordsToReturn.add(insertKafkaAvroBeforeProducerRecord);
         
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_UPDATE_EVENT)
      {
         GenericRecord kafkaAvroBeforeValueGenericRecord = kafkaKcopOperationIn
            .getKafkaAvroBeforeValueGenericRecord();
         GenericRecord kafkaAvroValueAuditBeforeRecord = createKafkaAuditAvroRecord(
            kafkaAvroBeforeValueGenericRecord,
            journalHeaderBefore,
            liveAuditAvroValueSchema);

         ProducerRecord<byte[], byte[]> insertKafkaAvroBeforeProducerRecord;

         insertKafkaAvroBeforeProducerRecord = createKafkaAuditAvroBinaryProducerRecord(
            kafkaAvroValueAuditBeforeRecord,
            kafkaKcopOperationIn,
            kafkaKcopHelperObjects);

         producerRecordsToReturn.add(insertKafkaAvroBeforeProducerRecord);

         GenericRecord kafkaAvroValueGenericRecord = kafkaKcopOperationIn.getKafkaAvroValueGenericRecord();
         GenericRecord kafkaAvroValueAuditRecord = createKafkaAuditAvroRecord(
            kafkaAvroValueGenericRecord,
            journalHeader,
            liveAuditAvroValueSchema);

         ProducerRecord<byte[], byte[]> insertKafkaAvroAfterProducerRecord;

         insertKafkaAvroAfterProducerRecord = createKafkaAuditAvroBinaryProducerRecord(
            kafkaAvroValueAuditRecord,
            kafkaKcopOperationIn,
            kafkaKcopHelperObjects);

         producerRecordsToReturn.add(insertKafkaAvroAfterProducerRecord);

      }

      return producerRecordsToReturn;
   }
   
   /**
    * Serialize the generic avro record using the Confluent serializer
    * 
    * @param kafkaAvroValueAuditRecord
    * @param kafkaKcopOperationIn
    * @param kafkaKcopHelperObjects
    * @return ProducerRecord
    */
   public ProducerRecord<byte[], byte[]> createKafkaAuditAvroBinaryProducerRecord(
      GenericRecord kafkaAvroValueAuditRecord,
      KafkaKcopOperationInIF kafkaKcopOperationIn,
      PersistentProducerObject kafkaKcopHelperObjects)
   {
      ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

      byte[] kafkaAvroKeyByteArray = kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord() == null ? new byte[0]
         : kafkaKcopHelperObjects.getConfluentKeySerializer().serialize(
            kafkaKcopOperationIn.getKafkaTopicName(),
            kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord());

      byte[] kafkaAvroValueByteArray = kafkaKcopHelperObjects.getConfluentValueSerializer().serialize(
         kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaAvroValueAuditRecord);

      insertKafkaAvroProducerRecord = new ProducerRecord<byte[], byte[]>(
         kafkaKcopOperationIn.getKafkaTopicName(),
         kafkaKcopOperationIn.getPartition(),
         (kafkaAvroKeyByteArray.length != 0) ? kafkaAvroKeyByteArray : null,
         (kafkaAvroValueByteArray.length != 0) ? kafkaAvroValueByteArray : null);

      return insertKafkaAvroProducerRecord;

   }

   /**
    * Creates a new Avro Generic Record that combines the original record for the source and 
    * the selected journal control fields.
    * 
    * @param kafkaAvroBeforeValueGenericRecord record
    * @param journalHeader journal control fields
    * @param kafkaAuditAvroSchema schema
    * @return generic avro record containing journal control fields
    * @throws UserExitException 
    */
   public Record createKafkaAuditAvroRecord(
      GenericRecord kafkaAvroBeforeValueGenericRecord,
      JournalHeaderIF journalHeader,
      Schema kafkaAuditAvroSchema) throws UserExitException
   {
      Record kafkaGenericAuditValueRecord = new GenericData.Record(kafkaAuditAvroSchema);

      int numFields = kafkaAvroBeforeValueGenericRecord.getSchema().getFields().size();
      for (int i = 0; i < numFields; i++)
      {
         kafkaGenericAuditValueRecord.put(i, kafkaAvroBeforeValueGenericRecord.get(i));
      }

      for (AvroJournalControlField avroJournalControlField : avroJournalControlFields)
      {
         JournalControlField journalControlField = avroJournalControlField.getJournalControlField();
         String journalControlFieldColumnName =journalControlField.columnName();
         
         String journalControlFieldValue = getJournalControlFieldValue(
            journalHeader,
            journalControlField);

         kafkaGenericAuditValueRecord.put(journalControlFieldColumnName, journalControlFieldValue);
      }
      return kafkaGenericAuditValueRecord;
   }
  
   private String getJournalControlFieldValue(
      JournalHeaderIF journalHeader,
      JournalControlField journalControlField)
   {
      switch (journalControlField)
      {
      case ENTTYP:
         return journalHeader.getEntryType();
      case JOBUSER:
         return journalHeader.getJobUser();
      case TIMSTAMP:
         return journalHeader.getTimestamp();
      case USER:
         return journalHeader.getUserName();
      case CCID:
         return journalHeader.getCommitID();
      case CNTRRN:
         return journalHeader.getRelativeRecNum();
      case CODE:
         return journalHeader.getJournalCode();
      case JOBNO:
         return journalHeader.getJobNumber();
      case LIBRARY:
         return journalHeader.getLibrary();
      case MEMBER:
         return journalHeader.getMemberName();
      case OBJECT:
         return journalHeader.getObjectName();
      case PROGRAM:
         return journalHeader.getProgramName();
      case SEQNO:
         return journalHeader.getSeqNo();
      case SYSTEM:
         return journalHeader.getSystemName();
      case UTC_TIMESTAMP:
         return journalHeader.getUtcTimestamp();
      default:
         return "";
      }
   }

   /**
    * 
    * @param kafkaKcopOperationIn -  Contains information relevant to the current operation being processed.
    * @param kafkaKcopCoordinator - Contains subscription scope information including storage for each thread's non-threadsafe objects.
    * @return - An object containing non-threadsafe objects.  Each thread executing createProducerRecords will create its own copy of this object and will be able to
    *           reuse it on that thread's subsequent calls to createProducerRecords as it will be stored in the kafkaKcopCoordinator, one for each thread.
    */
   private PersistentProducerObject createPersistentProducerObject(
      KafkaKcopOperationInIF kafkaKcopOperationIn,
      KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      KafkaAvroSerializer confluentKeySerializer;
      KafkaAvroSerializer confluentValueSerializer;
      Map<String, Object> propertiesMap = new Hashtable<String, Object>();

      // The parameter provided through the Kafka Custom Operation Processor, KCOP, interface.  In this example the parameter provided is the Schema Registry URL
      // which is used when instantiating the Confluent Avro Binary Serializers so that they can transparently contact.  Note in other scenarios this parameter could
      // represent the file location of a property list and processed in the init function.
      String baseSchemaRegistryUrl = getSchemaRegistryURL();

      // Create the two serializers this thread will make use of for each call to createProducerRecords.  Saving these serializer objecst avoids having to instantiate
      // each time.
      confluentKeySerializer = new KafkaAvroSerializer();
      confluentValueSerializer = new KafkaAvroSerializer();

      // Format the URL to make it suitable for the property that the Confluent serializer expects to be provided with.
      if (baseSchemaRegistryUrl.endsWith("/"))
      {
         propertiesMap.put(
            "schema.registry.url",
            baseSchemaRegistryUrl.substring(0, baseSchemaRegistryUrl.length() - 1));
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
      return (new PersistentProducerObject(
         confluentKeySerializer,
         confluentValueSerializer,
         baseSchemaRegistryUrl));
   }

   private void setSchemaRegistryURL(String providedURL)
   {
      schemaRegistryURL = providedURL;

   }

   private String getSchemaRegistryURL()
   {
      return schemaRegistryURL;
   }

   /**
    * Create a new schema for the audit record. This schema is used when creating the new generic avro record.
    * 
    * The journal control fields are guaranteed to return non-null values. 
    * 
    * Create a new list of fields for the new schema by taking fields from the previous schema and add 
    * to new schema, then create new fields for the audit record. Create a new schema with the new 
    * fields list and other parameters from the old schema.
    * 
    * @param kafkaKcopCoordinator
    * @param avroValueSchemaIn
    * @return schema
    */
   public Schema generateKafkaAuditValueSchema(
      Schema avroValueSchemaIn,
      ReplicationEventPublisherIF kafkaKcopCoordinator)
   {
      Schema kafkaAvroValueSchema = avroValueSchemaIn;
      List<Field> originalRecordSchemaFields = kafkaAvroValueSchema.getFields();
      List<Field> newRecordSchemaFields = new ArrayList<Field>();

      // Copy the schema from the original record into the schema for the new record
      for (Field f : originalRecordSchemaFields)
      {
         Schema.Field addExistingField = new Schema.Field(f.name(), f.schema(), f.doc(), f.defaultValue());
         newRecordSchemaFields.add(addExistingField);
      }

      // Create and add the fields for the journal control fields into the new schema
      for (AvroJournalControlField jcf : avroJournalControlFields)
      {
         Schema.Field addExistingField = new Schema.Field(jcf.getJcfSchemaField().name(), jcf.getJcfSchemaField().schema(), jcf.getJcfSchemaField().doc(), jcf.getJcfSchemaField().defaultValue());
         //newRecordSchemaFields.add(jcf.getJcfSchemaField());
         newRecordSchemaFields.add(addExistingField);
      }

      // Generate the new schema with the fields from the original record and the journal control fields
      Schema kafkaAuditAvroValueSchema = Schema.createRecord(
         kafkaAvroValueSchema.getName(),
         kafkaAvroValueSchema.getDoc(),
         kafkaAvroValueSchema.getNamespace(),
         kafkaAvroValueSchema.isError());
      kafkaAuditAvroValueSchema.setFields(newRecordSchemaFields);

      return kafkaAuditAvroValueSchema;
   }

   private Properties loadKCOPConfigurationProperties(
      String kcopParameter,
      ReplicationEventPublisherIF kafkaKcopCoordinator) throws UserExitException
   {
      FileInputStream configFileStream = null;
      String FILE_HEADER="-file:";
      Properties kafkaKcopConfigProperties = new Properties();
      
      if (    kcopParameter != null
          &&  kcopParameter.startsWith(FILE_HEADER))
      {
         String filename = kcopParameter.substring(FILE_HEADER.length());
         try
         {
            configFileStream = new FileInputStream(filename);
            kafkaKcopConfigProperties.load(configFileStream);
         }
         catch (FileNotFoundException e)
         {
            kafkaKcopCoordinator.logEvent("A properties file for " + this.getClass().getSimpleName()
               + " was not found. Default Journal Control Fields will be used for the audit fields.");
         }
         catch (IOException e)
         {
            kafkaKcopCoordinator
               .logEvent("An IOException was encountered when attempting to load the properties file provided by the user: " + kcopParameter);
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
      else if (    kcopParameter != null
               &&  kcopParameter.length() > 0)
      {
         kafkaKcopConfigProperties.put("schema.registry.url", kcopParameter);
      }
      else
      {
         // As a minimum we require a schema registry, and so we should immediately fail if no parameter is provided.
         kafkaKcopCoordinator.logEvent("No parameter was provided for the: "  + this.getClass().getSimpleName() + "KCOP.  One is required.  Stopping Replication");
         throw new UserExitException("No parameter was provided for the: "  + this.getClass().getSimpleName() + "KCOP.  One is required.  Stopping Replication");
      }
      return kafkaKcopConfigProperties;
   }

   private void processMappingProperties(
      Properties kCOPConfigurationProperties,
      ReplicationEventPublisherIF kafkaKCOPCoordinator) throws UserExitException
   {
      // The URL for the schema registry is provided in the properties file, it must always be obtained.
      setSchemaRegistryURL(kCOPConfigurationProperties.getProperty("schema.registry.url"));
      if (getSchemaRegistryURL() == null)
      {
         throw new UserExitException(
            "In the properties file provided via parameter to the: " +  this.getClass().getSimpleName() + ", schema.registry.url must be specified.");
      }

      String auditJCFs = kCOPConfigurationProperties.getProperty("audit.jcfs");
      if (auditJCFs != null)
      {
         
         avroJournalControlFields = buildListOfJournalControlFields(auditJCFs, kafkaKCOPCoordinator);
      }
      else
      {
         kafkaKCOPCoordinator.logEvent("audit.jcfs not specified, using default journal control fields instead.");
         avroJournalControlFields = buildListOfJournalControlFields(
            DEFAULT_AUDIT_COLUMNS,
            kafkaKCOPCoordinator);
      }

   }

   /**
    * Parse the specified journal control fields from the properties. Validate that the comma separated list all contain valid 
    * journal control field names.
    * 
    * @param csvSpecifiedJCFs specified journal control fields
    * @param kafkaKcopCoordinator coordinator
    * @return journal control fields
    * @throws UserExitException if any of the values are not valid journal control field names
    */
   private List<AvroJournalControlField> buildListOfJournalControlFields(
      String csvSpecifiedJCFs,
      ReplicationEventPublisherIF kafkaKcopCoordinator) throws UserExitException
   {
      JsonNode defaultString = getDefaultStringJsonNode();
      ArrayList<AvroJournalControlField> avroJournalControlFields = new ArrayList<AvroJournalControlField>();

      ArrayList<JournalControlField> journalControlFields = parseJournalControlFieldsFromConfig(csvSpecifiedJCFs);

      for (JournalControlField journalControlField : journalControlFields)
      {
         AvroJournalControlField avroJournalControlField = defineAvroJournalControlField(
            journalControlField,
            defaultString);

         avroJournalControlFields.add(avroJournalControlField);
      }
      return avroJournalControlFields;
   }

   private ArrayList<JournalControlField> parseJournalControlFieldsFromConfig(String csvJournalControlFields)
      throws UserExitException
   {
      String[] journalControlFieldsString = csvJournalControlFields.split(",");

      ArrayList<JournalControlField> journalControlFields = new ArrayList<JournalControlField>();

      for (String journalControlField : journalControlFieldsString)
      {
         try
         {
            journalControlFields.add(JournalControlField.valueOf(journalControlField));
         }
         catch (IllegalArgumentException e)
         {
            throw new UserExitException(
               journalControlField
                  + " is not a valid journal control field name. Please check the names of the journal control fields specified in the the properties file.");
         }
      }
      return journalControlFields;
   }
   
   private AvroJournalControlField defineAvroJournalControlField(
      JournalControlField journalControlField,
      JsonNode defaultString)
   {
      String journalControlFieldName = journalControlField.columnName();

      // Create fields for JCFs as string, since JCF values are returned as String and guaranteed to be non-null values
      Schema.Field auditSchemaField = new Schema.Field(
         journalControlFieldName,
         Schema.create(Type.STRING),
         null,
         defaultString);

      return new AvroJournalControlField(journalControlField, auditSchemaField);
   }

   private JsonNode getDefaultStringJsonNode() throws UserExitException
   {

      JsonNode defaultString;
      try
      {
         ObjectMapper mapper = new ObjectMapper();
         String defaultValueString = "\"\"";
         defaultString = mapper.readTree(defaultValueString);
      }
      catch (JsonProcessingException e)
      {
         throw new UserExitException("An unexpected error occurred.");
      }
      catch (IOException e)
      {
         throw new UserExitException("An unexpected error occurred.");
      }

      return defaultString;
   }

   /**
    * A class to hold the definiton of a journal control field to be used in an avro record
    */
   private class AvroJournalControlField
   {
      private JournalControlField avroSchemaColumnName;
      private Schema.Field schemaField;

      public AvroJournalControlField(JournalControlField journalControlField, Schema.Field schemaField)
      {
         this.avroSchemaColumnName = journalControlField;
         this.schemaField = schemaField;
      }

      public Schema.Field getJcfSchemaField()
      {
         return schemaField;
      }

      public JournalControlField getJournalControlField()
      {
         return avroSchemaColumnName;
      }
   }
   
   /**
    * An enum that defines all journal control fields that can be used in this user exit
    */
   public enum JournalControlField {
      ENTTYP("A_ENTTYP"),
      TIMSTAMP("A_TIMSTAMP"),
      USER("A_USER"), 
      JOBUSER("A_JOBUSER"),
      CCID("A_CCID"),
      CNTRRN("A_CNTRRN"),
      CODE("A_CODE"),
      JOBNO("A_JOBNO"),
      JOURNAL("A_JOURNAL"),
      LIBRARY("A_LIBRARY"),
      MEMBER("A_MEMBER"),
      OBJECT("A_OBJECT"),
      PROGRAM("_APROGRAM"),
      SEQNO("A_SEQNO"),
      SYSTEM("A_SYSTEM"),
      UTC_TIMESTAMP("A_UTC_TIMESTAMP"),
      ;
      
      private String columnName;

      JournalControlField(String columnName)
      {
         this.columnName = columnName;
      }

      public String columnName()
      {
         return columnName;
      }
   };

   /**
    * A convenient class which stores objects relevant to performing default behavior replication.  One of these objects will be created for each thread
    * processing source database events.   By virtue of persisting these objects we avoid having to instantiate on each call to createProducerRecords.
    * This class contains objects which are not thread-safe but since a copy will ultimately exists for each thread, we can make use of the objects.
    *
    */
   private class PersistentProducerObject
   {
      final KafkaAvroSerializer confluentKeySerializer;
      final KafkaAvroSerializer confluentValueSerializer;
      String baseSchemaRegistryUrl;
      public HashMap<String, Schema> topicToSchemaMap = new HashMap<String,Schema> ();

      public PersistentProducerObject(
         KafkaAvroSerializer aConfluentKeySerializer,
         KafkaAvroSerializer aConfluentValueSerializer,
         String aBaseSchemaRegistryUrl)
      {
         confluentKeySerializer = aConfluentKeySerializer;
         confluentValueSerializer = aConfluentValueSerializer;
         baseSchemaRegistryUrl = aBaseSchemaRegistryUrl;

      }

      public KafkaAvroSerializer getConfluentKeySerializer()
      {
         return confluentKeySerializer;
      }

      public KafkaAvroSerializer getConfluentValueSerializer()
      {
         return confluentValueSerializer;
      }

      public String getBaseSchemaRegistryUrl()
      {
         return baseSchemaRegistryUrl;
      }

   }

   @Override
   public void finish(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {

   }
}
