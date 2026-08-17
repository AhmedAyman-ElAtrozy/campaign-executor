import { Writer, SchemaRegistry, SCHEMA_TYPE_STRING } from 'k6/x/kafka';

const producer = new Writer({
  brokers: ['localhost:9092'],
  topic: 'campaign.audience',
  autoCreateTopic: false,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
  vus: 10,
  iterations: 2000,
};

export default function () {
  const customerId = `cust_sdfinal2_${__VU}_${__ITER}`;
  const message = {
    eventType: "AUDIENCE_RECORD",
    campaignId: "cmp_sdfinal2",
    customerId: customerId,
    msisdn: "+201012345678",
    email: "shutdown@example.com",
    language: "ar-EG",
    attributes: {}
  };

  producer.produce({
    messages: [
      {
        key: schemaRegistry.serialize({ data: customerId, schemaType: SCHEMA_TYPE_STRING }),
        value: schemaRegistry.serialize({ data: JSON.stringify(message), schemaType: SCHEMA_TYPE_STRING }),
      },
    ],
  });
}

export function teardown() {
  producer.close();
}