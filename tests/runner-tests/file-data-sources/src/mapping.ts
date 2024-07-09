import {
  ethereum,
  dataSource,
  BigInt,
  Bytes,
  DataSourceContext,
  store,
  log,
} from "@graphprotocol/graph-ts";
import { TestEvent } from "../generated/Contract/Contract";
import { FileEntity, Foo } from "../generated/schema";

const ONCHAIN_FROM_OFFCHAIN = "CREATE_ONCHAIN_DATASOURCE_FROM_OFFCHAIN_HANDLER";
const CREATE_FILE = "CREATE_FILE";
// const CREATE_FILE_FROM_HANDLE_FILE = "CREATE_FILE_FROM_HANDLE_FILE";
const CREATE_UNDEFINED_ENTITY = "CREATE_UNDEFINED_ENTITY";
const CREATE_CONFLICTING_ENTITY = "CREATE_CONFLICTING_ENTITY";
const SPAWN_FDS_FROM_OFFCHAIN_HANDLER = "SPAWN_FDS_FROM_OFFCHAIN_HANDLER";
const ACCESS_AND_UPDATE_OFFCHAIN_ENTITY_IN_ONCHAIN_HANDLER =
  "ACCESS_AND_UPDATE_OFFCHAIN_ENTITY_IN_ONCHAIN_HANDLER";
const ACCESS_FILE_ENTITY_THROUGH_DERIVED_FIELD =
  "ACCESS_FILE_ENTITY_THROUGH_DERIVED_FIELD";

const CREATE_FOO = "CREATE_FOO";
export function handleTestEvent(event: TestEvent): void {
  if (event.params.testCommand == CREATE_FILE) {
    dataSource.createWithContext(
      "File",
      [event.params.data],
      new DataSourceContext(),
    );
  }

  if (event.params.testCommand == SPAWN_FDS_FROM_OFFCHAIN_HANDLER) {
    let comma_separated_hash = event.params.data;
    let hash1 = comma_separated_hash.split(",")[0];
    let hash2 = comma_separated_hash.split(",")[1];
    let context = new DataSourceContext();
    context.setString("command", SPAWN_FDS_FROM_OFFCHAIN_HANDLER);
    context.setString("hash", hash2);

    log.info(
      "Creating file data source from handleFile, command : {} ,hash1: {}, hash2: {}",
      [SPAWN_FDS_FROM_OFFCHAIN_HANDLER, hash1, hash2],
    );
    dataSource.createWithContext("File", [hash1], context);
  }

  if (event.params.testCommand == ONCHAIN_FROM_OFFCHAIN) {
    let context = new DataSourceContext();
    context.setString("command", ONCHAIN_FROM_OFFCHAIN);
    context.setString("address", "0x0000000000000000000000000000000000000000");
    dataSource.createWithContext("File", [event.params.data], context);
  }

  if (event.params.testCommand == CREATE_UNDEFINED_ENTITY) {
    log.info("Creating undefined entity", []);
    let context = new DataSourceContext();
    context.setString("command", CREATE_UNDEFINED_ENTITY);
    dataSource.createWithContext("File", [event.params.data], context);
  }

  if (event.params.testCommand == CREATE_CONFLICTING_ENTITY) {
    log.info("Creating conflicting entity", []);
    let entity = new FileEntity(event.params.data);
    entity.content = "content";
    entity.save();
  }

  if (
    event.params.testCommand ==
    ACCESS_AND_UPDATE_OFFCHAIN_ENTITY_IN_ONCHAIN_HANDLER
  ) {
    let hash = event.params.data;
    log.info("Creating file data source from handleFile: {}", [hash]);
    let entity = FileEntity.load(event.params.data);
    if (entity == null) {
      log.info("Entity not found", []);
    } else {
      // This should never be logged if the entity was created in the offchain handler
      // Such entities are not accessible in onchain handlers and will return null on load
      log.info("Updating entity content", []);
      entity.content = "updated content";
      entity.save();
    }
  }

  if (event.params.testCommand == CREATE_FOO) {
    let entity = new Foo(event.params.data);
    entity.save();
    let context = new DataSourceContext();
    context.setString("command", CREATE_FOO);
    dataSource.createWithContext("File", [event.params.data], context);
  }

  if (event.params.testCommand == ACCESS_FILE_ENTITY_THROUGH_DERIVED_FIELD) {
    let entity = Foo.load(event.params.data);
    if (entity == null) {
      log.info("Entity not found", []);
    } else {
      log.info("Accessing file entity through derived field", []);
      let fileEntity = entity.ipfs.load();

      assert(fileEntity.length == 0, "Expected exactly one file entity");
    }
  }
}

export function handleFile(data: Bytes): void {
  log.info('handleFile {}', [dataSource.stringParam()]);
  let context = dataSource.context();

  if (!context.isSet('command')) {
    log.info('Creating FileEntity from handleFile: {} , content : {}', [
      dataSource.stringParam(),
      data.toString(),
    ]);

    let entity = new FileEntity(dataSource.stringParam());
    entity.content = data.toString();
    entity.save();

    return;
  }

  let contextCommand = context.getString('command');

  if (contextCommand == SPAWN_FDS_FROM_OFFCHAIN_HANDLER) {
    let hash = context.getString('hash');
    log.info('Creating file data source from handleFile: {}', [hash]);
    dataSource.createWithContext('File', [hash], new DataSourceContext());
  } else if (contextCommand == ONCHAIN_FROM_OFFCHAIN) {
    log.info('Creating onchain data source from offchain handler', []);
    let address = context.getString('address');
    dataSource.create('OnChainDataSource', [address]);
  } else if (contextCommand == CREATE_UNDEFINED_ENTITY) {
    log.info('Creating undefined entity', []);
    let entity = new Foo(dataSource.stringParam());
    entity.save();
  } else if (contextCommand == CREATE_FOO) {
    log.info('Creating FileEntity with relation to Foo', []);
    let entity = new FileEntity(dataSource.stringParam());
    entity.foo = dataSource.stringParam();
    entity.content = data.toString();
    entity.save();
  }
}