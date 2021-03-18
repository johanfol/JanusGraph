import * as kafka from 'kafka-node';
import { log } from '../helper';
import { publish } from '../producer/producer';
import gremlin from 'gremlin';

const handleQuoteMessage = async (message: kafka.Message, topic: string) => {
  const jMessage = JSON.parse(message.value.toString());
  const client = new gremlin.driver.Client('ws://localhost:8182/gremlin', { traversalSource: 'g' });

  const txID: string = jMessage.transactionId;
  const msisdn: string = jMessage.payer.msisdn;
  log(`Handling quote message with TXID ${jMessage.transactionId}`, topic);
  // Write required logic here
  const result = await client.submit('g.V(msisdn).as("a").repeat(out().simplePath()).times(3).where(out().as("a")).path().dedup().by(unfold().order().by(id).dedup().fold())', {msisdn: msisdn});
  const transactionBetweenParties = result.lenght > 0;

  publish(
    topic,
    `Transaction: ${txID} is ${transactionBetweenParties ? '' : 'not'} in set`,
  );
};

export default handleQuoteMessage;
