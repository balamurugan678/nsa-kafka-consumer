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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopReplicationCoordinatorIF;
import com.datamirror.ts.target.publication.userexit.ReplicationEventTypes;
import com.datamirror.ts.target.publication.userexit.UserExitException;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaCustomOperationProcessorIF;
import com.datamirror.ts.target.publication.userexit.kafka.KafkaKcopOperationInIF;

/**
 * 
 * <p>This KCOP replicates operations on the source to Kafka using JSON encoded Avro.  The jsonEncoder from
 * the Apache Avro library is made use of to perform the serilaization.  By virtue of using JSON the
 * requirement for a schema registry is removed and thus any installation of Apache Kafka can be the
 * target of replication without the need to use consumers with special deserializers or a schema
 * registry.</p>
 * 
 * <p>The KcopJsonFormatIntegrated requires no parameters, simply supply the fully qualified KCOP name
 * in Management Console.  The resulting topic will have "-json" appended. </p>
 * 
 * <p>The KcopJsonFormatIntegrated avoids having to use a schema registry and associated
 * deserializer is simpler to implement initally.  HOWEVER, it has several drawbacks, there is a reason
 * the default CDC Target behaiour is to leverage a schema registry.</p>
 * 
 * <p>Using JSON suffers performance penalties as compared to the Avro Binary methods of replicating to
 * Kafka.   Several factors contribute to this, the Kafka record size is larger as it requires the schema
 * to be encoded.  Binary fields are replicated to base64 which requries more bytes. The serialization process
 * is slower in its string manipulation.  Deserialization requires UTF-8 text parsing vs. a memory copy in the
 * binary case.  The resulting object from deserialization in the Avro Binary methodology is a java object
 * which can then be manipulated for simple transformation.</p>
 * 
 * <p>JSON does not deal well with placing special values like NaN into doubles as this is a limitation of the
 * JSON format.</p>
 * 
 * <p>Our recommendation is to leverage a schema registry, even if the ultimate intent is to produce JSON in the
 * final datalake for example.  That being said there is a convenience factor to leveraging this KCOP and so
 * it is provided.</p>
 * 
 * <p> NOTE 1:  The createProducerRecords class is not thread safe.  However a means is provided so that each thread
 *   can store its own copy of non-theadsafe objects.  Please see how this is done In the KcopDefaultBehaviorIntegrated
 *   as it is not specifically required in this KCOP.</p>
 *    
 * <p> NOTE 2:  The records returned by createProducerRecords are not deep copied, so each call to the method should
 *   generate new records and not attempt to keep references to old ones for reuse.</p>
 *   
 * <p> Note 3:  The KCOP is instantiated once per subscription which registers the KCOP.  This means if statics are made
 *   use of, they will potentially be shared across the instantiated KCOPs belonging to multiple actively replicating 
 *   subscriptions.</p>
 */
public class KcopJsonFormatIntegrated implements KafkaCustomOperationProcessorIF
{

   public static final int CONFLUENT_SCHEMA_REGISTRY_CACHE_SIZE = 5000;
   

   @Override
   public void init(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
    
      //Subscribe to some operations
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_INSERT_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_DELETE_EVENT);
      kafkaKcopCoordinator.subscribeEvent(ReplicationEventTypes.BEFORE_UPDATE_EVENT); 
      
      // log an event indicating the Kcop to be used for formatting coordinator logging function.
      kafkaKcopCoordinator.logEvent("The Kafka Custom Operation Processor, " + this.getClass().getSimpleName() + ", will be used for operation formatting."); 
      
      if (    kafkaKcopCoordinator.getParameter() != null
          &&  kafkaKcopCoordinator.getParameter().length() != 0)
      {
         kafkaKcopCoordinator.logEvent ("The " + this.getClass().getSimpleName() + " does not utilize any parameters.  Please do not provide any.  Stopping replication.");
         throw new UserExitException ("The " + this.getClass().getSimpleName() + " does not utilize any parameters.  Please do not provide any.  Stopping replication.");
      }
   }

   @Override
   public ArrayList<ProducerRecord<byte[], byte[]>> createProducerRecords(
      KafkaKcopOperationInIF kafkaKcopOperationIn,
      KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator) throws UserExitException
   {
      
      ArrayList<ProducerRecord<byte[], byte[]>> producerRecordsToReturn = new ArrayList<ProducerRecord<byte[], byte[]>>();

      byte[] kafkaJSONKeyByteArray;
      try
      {
         kafkaJSONKeyByteArray = kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord() == null ? new byte[0] : encodeMessageJson(kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord().getSchema(),
                                                                                                                               kafkaKcopOperationIn.getKafkaAvroKeyGenericRecord());
      }
      catch (IOException e)
      {
         throw new UserExitException(e.getMessage());
      }
      
      // Adding the notation -json to the topic to denote that it should be consumed by a byte array deserializer
      // and is in json format.
      String jsonTopicName = kafkaKcopOperationIn.getKafkaTopicName() + "-json";
      Integer jsonPartitionToWriteTo = 0;
          
      if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_INSERT_EVENT)
      {
         ProducerRecord<byte[], byte[]> insertKafkaJSONProducerRecord;
         
         byte[] kafkaJSONValueByteArray;
         try
         {
            kafkaJSONValueByteArray = kafkaKcopOperationIn.getKafkaAvroValueGenericRecord() == null ? new byte[0] : encodeMessageJson(kafkaKcopOperationIn.getKafkaAvroValueGenericRecord().getSchema(),
                                                                                                                                      kafkaKcopOperationIn.getKafkaAvroValueGenericRecord());
         }
         catch (IOException e)
         {
            throw new UserExitException(e.getMessage());         
         }
            
         
         insertKafkaJSONProducerRecord = new ProducerRecord<byte[], byte[]> 
         (
            jsonTopicName,
            jsonPartitionToWriteTo,
            (kafkaJSONKeyByteArray.length != 0) ? kafkaJSONKeyByteArray : null,
            (kafkaJSONValueByteArray.length != 0) ? kafkaJSONValueByteArray : null          
         );      
         producerRecordsToReturn.add(insertKafkaJSONProducerRecord);
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_DELETE_EVENT)
      {
         ProducerRecord<byte[], byte[]> deleteKafkaJSONProducerRecord;
                
         deleteKafkaJSONProducerRecord = new ProducerRecord<byte[], byte[]>
         (
            jsonTopicName,
            jsonPartitionToWriteTo,
            ( kafkaJSONKeyByteArray.length != 0) ? kafkaJSONKeyByteArray : null,
            null          
         ); 
         
         producerRecordsToReturn.add(deleteKafkaJSONProducerRecord);
      }
      else if (kafkaKcopOperationIn.getReplicationEventType() == ReplicationEventTypes.BEFORE_UPDATE_EVENT)
      {


         
         // Now Do the records in JSON format for the JSON topics
         if (kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord() != null)
         {
            // The Key has changed so we need to delete the record associated with the old key.
            byte[] kafkaJSONKeyUpdatedByteArray;
            try{
               kafkaJSONKeyUpdatedByteArray= encodeMessageJson(kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord().getSchema(), kafkaKcopOperationIn.getKafkaAvroUpdatedKeyGenericRecord());
            }
            catch (IOException e)
            {
               throw new UserExitException(e.getMessage());         
            }
            
            ProducerRecord<byte[], byte[]> deleteKafkaJSONProducerRecord;
            deleteKafkaJSONProducerRecord = new ProducerRecord<byte[], byte[]>
            (
               jsonTopicName,
               jsonPartitionToWriteTo,
               ( kafkaJSONKeyUpdatedByteArray.length != 0) ? kafkaJSONKeyUpdatedByteArray : null,
               null          
            ); 
            
            producerRecordsToReturn.add(deleteKafkaJSONProducerRecord);
         }
        
         // In the case that the Kafka Key columns were not part of the update, an insert is sufficient.
         ProducerRecord<byte[], byte[]> insertKafkaJSONProducerRecord;
         byte[] kafkaJSONValueByteArray;
         try
         {
            kafkaJSONValueByteArray = kafkaKcopOperationIn.getKafkaAvroValueGenericRecord() == null ? new byte[0] :  encodeMessageJson(kafkaKcopOperationIn.getKafkaAvroValueGenericRecord().getSchema(), 
                                                                                                                                       kafkaKcopOperationIn.getKafkaAvroValueGenericRecord() );
         }
         catch (IOException e)
         {
            throw new UserExitException(e.getMessage());         
         }
         
         insertKafkaJSONProducerRecord = new ProducerRecord<byte[], byte[]> 
         (
            jsonTopicName,
            jsonPartitionToWriteTo,
            (kafkaJSONKeyByteArray.length != 0) ? kafkaJSONKeyByteArray : null,
            (kafkaJSONValueByteArray.length != 0) ? kafkaJSONValueByteArray : null          
         );      
         producerRecordsToReturn.add(insertKafkaJSONProducerRecord);
      }
      
      return producerRecordsToReturn;

   }
   
   /**
    * Makes use of the Avro jsonEncoder method to convert an Avro Generic Record into
    * Json encoded Avro.
    * 
    * @param schema - The schema to encode into the resultant JSON
    * @param avroRecord - Contains the column data values of the row.
    * @return
    * @throws IOException
    */
   private static byte[] encodeMessageJson(Schema schema, Object avroRecord) throws IOException
   {

      // Create byteArray stream
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      // Create Encoder
      JsonEncoder je = null;
      je = EncoderFactory.get().jsonEncoder(schema, out);

      // Write the message
      DatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);
      if (avroRecord instanceof SpecificRecord)
      {
         writer = new SpecificDatumWriter<Object>();
         writer.setSchema(schema);
      }
      writer.write(avroRecord, je);
      je.flush();
      out.close();

      return out.toByteArray();
   }

   @Override
   public void finish(KafkaKcopReplicationCoordinatorIF kafkaKcopCoordinator)
   {

   }

}
