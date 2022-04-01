import { ArchipelagoController, defaultArchipelagoController, IslandUpdates, UpdateSubscriber } from "@dcl/archipelago";
import { connect, JSONCodec, NatsConnection, StringCodec, Subscription } from "nats";

async function main(){
  const controller = defaultArchipelagoController({
    flushFrequency: 2.0,
    archipelagoParameters: {
      joinDistance: 64,
      leaveDistance: 80,
      maxPeersPerIsland: 100
    }
  })

  const natsAddress = process.env.NATS_ADDRESS
  const server = { servers: natsAddress }
  try {
    const nc = await connect(server);
    const sc = StringCodec();
    const jc = JSONCodec();
    console.log(`connected to ${nc.getServer()}`);
    // this promise indicates the client closed
    const done = nc.closed();

  const registerSubscribe = (subject: string, cb: Function) => {
    const subscription = nc.subscribe(subject);
    (async () => {
      for await (const message of subscription) {
        try {
          if (message.data.length) {
            const data = jc.decode(message.data)
            console.log(`[${subscription.getProcessed()}]: ${message.subject}: ${JSON.stringify(data, null, 2)}`);
            await cb.bind(controller)(data);
          } else {
            console.log(`[${subscription.getProcessed()}]: ${message.subject}`);
            await cb.bind(controller)();
          }
        } catch (err) {
          console.log(err);
        }
      }
    })();
  }

  const registerResponse = (subject: string, cb: Function) => {
    const subscription = nc.subscribe(subject);
    (async () => {
      for await (const message of subscription) {
        try {
          if (message.data.length) {
            const data = jc.decode(message.data)
            console.log(`[${subscription.getProcessed()}]: ${message.subject}: ${JSON.stringify(data, null, 2)}`);
            const payload = await cb.bind(controller)(data);
            console.log(payload)
            message.respond(jc.encode(payload));
          } else {
            console.log(`[${subscription.getProcessed()}]: ${message.subject}`);
            const payload = await cb.bind(controller)();
            console.log(payload)
            message.respond(jc.encode(payload));
          }
        } catch (err) {
          console.log(err);
        }
      }
    })();
  }

  // Publish
  const onIslandUpdates = async (updates: IslandUpdates) => {
    const strUpdates = JSON.stringify(updates)
    nc.publish('archipelago.subscribeToUpdates', sc.encode(strUpdates));
    console.log(`archipelago.subscribeToUpdates: ${strUpdates}`);
  }
  controller.subscribeToUpdates(onIslandUpdates.bind(controller))

  // Subscribe
  registerSubscribe('archipelago.setPeersPositions', controller.setPeersPositions)
  registerSubscribe('archipelago.dispose', controller.dispose)
  registerSubscribe('archipelago.flush', controller.flush)
  registerSubscribe('archipelago.clearPeers', controller.clearPeers)
  registerSubscribe('archipelago.modifyOptions', controller.modifyOptions)

  // Req/Res
  registerResponse('archipelago.getPeersCount', controller.getPeersCount)
  registerResponse('archipelago.getIslandsCount', controller.getIslandsCount)
  registerResponse('archipelago.getPeerData', controller.getPeerData)
  registerResponse('archipelago.getPeersData', controller.getPeersData)
  registerResponse('archipelago.getIsland', controller.getIsland)
  registerResponse('archipelago.getIslands', controller.getIslands)

  } catch (err) {
    console.log(`error connecting to ${JSON.stringify(server)}`);
  }
}

main()