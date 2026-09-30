import { Writer, SchemaRegistry, SCHEMA_TYPE_STRING } from 'k6/x/kafka';

const producer = new Writer({
  brokers: ['localhost:9092'],
  topic: 'campaign.audience',
  autoCreateTopic: false,
});

const schemaRegistry = new SchemaRegistry();

export const options = {
  vus: 10,
  iterations: 11000,
};

export default function () {
  const i = __ITER;
  let msisdn = "";
  let email = "";

  if (i < 8000) {
    msisdn = `+2010${String(10000000 + i).padStart(8, '0')}`;
  } else if (i < 10000) {
    email = `demo_${i}@example.com`;
  }

  const customerId = `cust_poll_fix_${i}`;
  const message = {
    eventType: "AUDIENCE_RECORD",
    campaignId: "cmp_poll_fix",
    customerId: customerId,
    msisdn: msisdn,
    email: email,
    language: "ar-EG",
    attributes: {}
  };

  producer.produce({
    messages: [
      {
        key: schemaRegistry.serialize({ data: "cmp_poll_fix", schemaType: SCHEMA_TYPE_STRING }),
        value: schemaRegistry.serialize({ data: JSON.stringify(message), schemaType: SCHEMA_TYPE_STRING }),
      },
    ],
  });
}

export function teardown() {
  producer.close();
}