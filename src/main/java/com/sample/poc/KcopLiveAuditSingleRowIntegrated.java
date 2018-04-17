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
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.datamirror.ts.target.publication.userexit.JournalHeaderIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventPublisherIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaCustomOperationProcessorIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;

/**
 *  <p>This sample produces audit records in CSV format. 
 *  Please see the examples below for how an insert, update and delete will appear below.</p>
 * 
 * Example Insert record of "1, 'abc'" into the source database:
 * <br>2017-09-14 12:00:00.000000,I,\N,\N,1,abc
 * <br>Example Delete of the inserted record above:
 * <br>2017-09-14 12:00:00.000000,D,1,abc,\N,\N
 * <br>Example Update of a record, where the row "1,'abc'" had 'abc' updated to 'def':
 * <br>2017-09-14 12:00:00.000000,U,1,abc,1,def
 * <p>
 *  The input parameter is an optional properties file. The properties file
 *  can be specified by using -file:[properties_file_path] in the user exit parameter.</p>
 *  
 *  <p>The properties file can define the following parameters:
 *  <br>
 *  <br>schema.registry - the schema registry url
 *  <br>character.delimiter - the character that encloses the values of the output record - " (double quote) by default
 *  <br>field.delimiter - the character that delimits the fields of the output record - , (comma) by default)</p>

 *  <p>By default, the values are comma separated and the fields are quote encased in quotes. If different
 *  delimiters are desired, create a properties file with the above paramters defined. e.g, to specify 
 *  the field delimiter as | and the character delimiter as ', create a properties file containing:</p>
 *  
 *  schema.registry=https://[schema_registry_url]:[schema_registry_port]
 *  <br>character.delimiter=,
 *  <br>field.delimiter=|
 *  
 *  <p>The specific journal control fields can be modified to include different journal control fields if desired. 
 *  Please see {@link KcopLiveAuditSingleRowIntegrated#appendJournalControlFields} to modify the journal control fields used in the output. </p>
 *  
 *  <p>NOTE 1:  The createProducerRecords class is not thread safe.  However a means is provided so that each thread
 *   can store its own copy of non-theadsafe objects.  Please see KCopDefaultBehaviourIntegrated for how this is done.</p>
 *    
 *  <p>NOTE 2:  The records returned by createProducerRecords are not deep copied, so each call to the method should
 *   generate new records and not attempt to keep references to old ones for reuse.</p>
 *   
 *  <p>Note 3:  The KCOP is instantiated once per subscription which registers the KCOP.  This means if statics are made
 *   use of, they will potentially be shared across the instantiated KCOPs belonging to multiple actively replicating 
 *   subscriptions.</p>
 *  
 */
public class KcopLiveAuditSingleRowIntegrated implements KafkaCustomOperationProcessorIF
{

   public static final int CONFLUENT_SCHEMA_REGISTRY_CACHE_SIZE = 5000;

   private static final String DEFAULT_FIELD_DELIMITER = ",";
   private static final String ESCAPE_CHAR = "\\";
   private static final String ESCAPE_ESCAPE_CHAR = "\\\\";
   private static final String NULL_ESCAPE = "\\N";

   private String fieldDelimiter;

   private Properties kCOPConfigurationProperties;

   @Override
   public void init(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      //Subscribe to some operations
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_INSERT_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_DELETE_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_UPDATE_EVENT);
      
      //Parse the properties file if one is specified.
      kCOPConfigurationProperties = loadKCOPConfigurationProperties(
         kafkaKcopCoordinator.getParameter(),
         kafkaKcopCoordinator);

      // Validate and process the contents of the property file if one was provided.
      processMappingProperties(kCOPConfigurationProperties, kafkaKcopCoordinator);
   }

   @Override
   public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords(
      KafkaKcopOperationInIF kafkaKCOPOperationIn,
      KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<ProducerRecord<byte[], byte[]>>();

      JournalHeaderIF journalHeader = kafkaKCOPOperationIn.getUserExitJournalHeader();

      GenericRecord kafkaAvroValueGenericRecord = kafkaKCOPOperationIn.getKafkaAvroValueGenericRecord();
      GenericRecord kafkaAvroBeforeValueGenericRecord = kafkaKCOPOperationIn.getKafkaAvroBeforeValueGenericRecord();

      String kafkaValueAuditRecord = createKafkaAuditRecord(
         kafkaAvroValueGenericRecord,
         kafkaAvroBeforeValueGenericRecord,
         journalHeader);

      ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

      insertKafkaAvroProducerRecord = createKafkaAuditBinaryProducerRecord(
         kafkaValueAuditRecord,
         kafkaKCOPOperationIn);

      producerRecordsToReturn.add(insertKafkaAvroProducerRecord);

      return producerRecordsToReturn;
   }

   /**
    * <p>Generates a flat audit record with the timestamp, entry type, the before image and after image.
    * <br>I = Insert
    * <br>U = Update
    * <br>D = Delete</p>
    * 
    * <p>2017-09-14 12:00:00.000000,I,\N,\N,1,abc
    * <br>2017-09-14 12:00:00.000000,D,1,abc,\N,\N
    * <br>2017-09-14 12:00:00.000000,U,1,abc,1,updated</p>
    * 
    * 
    * @param kafkaGenericAvroValueRecord
    * @param kafkaGenericAvroValueBeforeRecord
    * @param journalHeader
    * @return flat audit record
    */
   private String createKafkaAuditRecord(
      GenericRecord kafkaGenericAvroValueRecord,
      GenericRecord kafkaGenericAvroValueBeforeRecord,
      JournalHeaderIF journalHeader)
   {
      StringBuilder kafkaGenericFlatAuditRecord = new StringBuilder();

      // add the journal control fields
      appendJournalControlFields(journalHeader, kafkaGenericFlatAuditRecord);

      // Write the before image to the record (deletes, updates),
      // or add delimiter placeholders for the number of after image fields if before image doesn't exist (inserts)
      if (kafkaGenericAvroValueBeforeRecord == null)
      {
         int numberBeforeImageFields = kafkaGenericAvroValueRecord.getSchema().getFields().size();
         for (int i = 0; i < numberBeforeImageFields; i++)
         {
            kafkaGenericFlatAuditRecord.append(NULL_ESCAPE + fieldDelimiter);
         }
      }
      else
      {
         int numberBeforeImageFields = kafkaGenericAvroValueBeforeRecord.getSchema().getFields().size();

         formatRecord(kafkaGenericAvroValueBeforeRecord, kafkaGenericFlatAuditRecord, numberBeforeImageFields);
         kafkaGenericFlatAuditRecord.append(fieldDelimiter);
      }

      // Write the after image to the record (inserts, updates),
      // or add delimiter placeholders for the number of before image fields if after image doesn't exist (deletes)
      if (kafkaGenericAvroValueRecord == null)
      {
         int numberAfterImageFields = kafkaGenericAvroValueBeforeRecord.getSchema().getFields().size();
         for (int i = 0; i < numberAfterImageFields; i++)
         {
            kafkaGenericFlatAuditRecord.append(NULL_ESCAPE);
            if (i < numberAfterImageFields - 1)
            {
               kafkaGenericFlatAuditRecord.append(fieldDelimiter);
            }
         }
      }
      else
      {
         int numberAfterImageFields = kafkaGenericAvroValueRecord.getSchema().getFields().size();
         formatRecord(kafkaGenericAvroValueRecord, kafkaGenericFlatAuditRecord, numberAfterImageFields);
      }

      return kafkaGenericFlatAuditRecord.toString();
   }

   /**
    * This method formats the Avro Generic Record. This method will escape delimiters, escape characters and
    * Base64 encode binary data.
    * 
    * @param kafkaGenericAvroValueBeforeRecord
    * @param kafkaGenericFlatAuditRecord
    * @param numberAfterImageFields
    */
   private void formatRecord(
      GenericRecord kafkaGenericAvroValueBeforeRecord,
      StringBuilder kafkaGenericFlatAuditRecord,
      int numberAfterImageFields)
   {
      List<Field> fields = kafkaGenericAvroValueBeforeRecord.getSchema().getFields();
      for (int i = 0; i < numberAfterImageFields; i++)
      {
         Field field = fields.get(i);
         Schema fieldSchema = field.schema();
         Type type = fieldSchema.getType();
         Object record = kafkaGenericAvroValueBeforeRecord.get(i);
         String value;

         if (type == Type.STRING
            || (type == Type.UNION && schemaContainsStringSchema(fieldSchema.getTypes())))
         {
            if (record == null)
            {
               value = NULL_ESCAPE;
            }
            else
            {
               value = record.toString().replace(ESCAPE_CHAR, ESCAPE_ESCAPE_CHAR);
               value = value.replace(fieldDelimiter, ESCAPE_CHAR + fieldDelimiter);
            }
         }
         else if (type == Type.BYTES)
         {
            value = Base64.encodeBase64String(record.toString().getBytes());
         }
         else
         {
            value = record.toString();
         }

         kafkaGenericFlatAuditRecord.append(value);
         if (i != numberAfterImageFields - 1)
         {
            kafkaGenericFlatAuditRecord.append(fieldDelimiter);
         }
      }
   }

   private boolean schemaContainsStringSchema(List<Schema> unionSchema)
   {
      boolean containsStringSchema = false;
      for (Schema schema : unionSchema)
      {
         if (schema.getType() == Type.STRING)
            containsStringSchema = true;
      }

      return containsStringSchema;
   }

   /**
    * <p>This method appends the desired journal control fields to the record. Modify this 
    * method to specify the journal control fields written the record.</p>
    * 
    * @param journalHeader
    * @param kafkaGenericFlatAuditRecord
    */
   public void appendJournalControlFields(
      JournalHeaderIF journalHeader,
      StringBuilder kafkaGenericFlatAuditRecord)
   {
      // Write the timestamp and entty to the record
      kafkaGenericFlatAuditRecord
         .append(getJournalControlField(journalHeader, JournalControlField.TIMSTAMP))
         .append(fieldDelimiter);

      kafkaGenericFlatAuditRecord
         .append(getJournalControlField(journalHeader, JournalControlField.CCID))
         .append(fieldDelimiter);

      kafkaGenericFlatAuditRecord.append(
         translateEnttyp(getJournalControlField(journalHeader, JournalControlField.ENTTYP))).append(
         fieldDelimiter);

      kafkaGenericFlatAuditRecord
         .append(getJournalControlField(journalHeader, JournalControlField.USER))
         .append(fieldDelimiter);
   }

   private String getJournalControlField(
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
      case PARTITION:
      default:
         return "";
      }
   }

   private ProducerRecord<byte[], byte[]> createKafkaAuditBinaryProducerRecord(
      String liveAuditRecord,
      KafkaKcopOperationInIF kafkaKCOPOperationIn)
   {
      ProducerRecord<byte[], byte[]> insertKafkaAvroProducerRecord;

      byte[] kafkaAvroValueByteArray = liveAuditRecord.getBytes();

      insertKafkaAvroProducerRecord = new ProducerRecord<byte[], byte[]>(
         kafkaKCOPOperationIn.getKafkaTopicName() + "-audit",
         kafkaKCOPOperationIn.getPartition(),
         null,
         (kafkaAvroValueByteArray.length != 0) ? kafkaAvroValueByteArray : null);

      return insertKafkaAvroProducerRecord;

   }

   private String translateEnttyp(String enttyp)
   {
      String convertedEvent;
      if ("PT".equals(enttyp) || "RR".equals(enttyp))
      {
         convertedEvent = "I";
      }
      else if ("UP".equals(enttyp))
      {
         convertedEvent = "U";
      }
      else if ("DL".equals(enttyp))
      {
         convertedEvent = "D";
      }
      else
      {
         convertedEvent = "R";
      }

      return convertedEvent;
   }

   private Properties loadKCOPConfigurationProperties(
      String kcopParameter,
      KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      FileInputStream configFileStream = null;
      String FILE_HEADER = "-file:";
      Properties kafkaKcopConfigProperties = new Properties();

      // It is acceptable for either a properties file is provided or no parameter to be specified, thus using defaults.
      if (   kcopParameter != null
          && kcopParameter.startsWith(FILE_HEADER))
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
               + " was not found. Default character and field delimiters will be used.");
         }
         catch (IOException e)
         {
            kafkaKcopCoordinator
               .logEvent("An IOException was encountered when attempting to load the properties file provided by the user.");
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
               &&  kcopParameter.length() != 0)
      {
         kafkaKcopCoordinator.logEvent("An invalid parameter was provided: " + kcopParameter  + " Provide a file with the prefix: " + FILE_HEADER + " or do not specify a parameter.");
         throw new UserExitException("An invalid parameter was provided: " + kcopParameter  + " Provide a file with the prefix: " + FILE_HEADER + " or do not specify a parameter.");
      }
      return kafkaKcopConfigProperties;
   }

   private void processMappingProperties(
      Properties kCOPConfigurationProperties,
      ReplicationEventPublisherIF kafkaKcopCoordinator) throws UserExitException
   {
      setFieldDelimiter(kCOPConfigurationProperties.getProperty("field.delimiter"));
      if (getFieldDelimiter() == null)
      {
         setFieldDelimiter(DEFAULT_FIELD_DELIMITER);
      }
   }

   private String getFieldDelimiter()
   {
      return fieldDelimiter;
   }

   private void setFieldDelimiter(String fieldDelimiter)
   {
      this.fieldDelimiter = fieldDelimiter;

   }

   @Override
   public void finish(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {
      // TODO Auto-generated method stub

   }

   /**
    * An enum that defines all journal control fields that can be used in this user exit
    */
   public enum JournalControlField {
      ENTTYP, TIMSTAMP, USER, JOBUSER, CCID, CNTRRN, CODE, JOBNO, JOURNAL, LIBRARY, MEMBER, OBJECT, PROGRAM, SEQNO, PARTITION, SYSTEM, UTC_TIMESTAMP, ;

   };
}
