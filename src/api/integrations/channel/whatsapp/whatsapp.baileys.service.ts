import { getCollectionsDto } from '@api/dto/business.dto';
import { OfferCallDto } from '@api/dto/call.dto';
import {
  ArchiveChatDto,
  BlockUserDto,
  DeleteMessage,
  getBase64FromMediaMessageDto,
  LastMessage,
  MarkChatUnreadDto,
  NumberBusiness,
  OnWhatsAppDto,
  PrivacySettingDto,
  ReadMessageDto,
  SendPresenceDto,
  UpdateMessageDto,
  WhatsAppNumberDto,
} from '@api/dto/chat.dto';
import {
  AcceptGroupInvite,
  CreateGroupDto,
  GetParticipant,
  GroupDescriptionDto,
  GroupInvite,
  GroupJid,
  GroupPictureDto,
  GroupSendInvite,
  GroupSubjectDto,
  GroupToggleEphemeralDto,
  GroupUpdateParticipantDto,
  GroupUpdateSettingDto,
} from '@api/dto/group.dto';
import { InstanceDto, SetPresenceDto } from '@api/dto/instance.dto';
import { HandleLabelDto, LabelDto } from '@api/dto/label.dto';
import {
  Button,
  ContactMessage,
  KeyType,
  MediaMessage,
  Options,
  SendAudioDto,
  SendButtonsDto,
  SendContactDto,
  SendListDto,
  SendLocationDto,
  SendMediaDto,
  SendPollDto,
  SendPtvDto,
  SendReactionDto,
  SendStatusDto,
  SendStickerDto,
  SendTextDto,
  StatusMessage,
  TypeButton,
} from '@api/dto/sendMessage.dto';
import { chatwootImport } from '@api/integrations/chatbot/chatwoot/utils/chatwoot-import-helper';
import * as s3Service from '@api/integrations/storage/s3/libs/minio.server';
import { ProviderFiles } from '@api/provider/sessions';
import { PrismaRepository, Query } from '@api/repository/repository.service';
import { chatbotController, waMonitor } from '@api/server.module';
import { CacheService } from '@api/services/cache.service';
import { ChannelStartupService } from '@api/services/channel.service';
import { Events, MessageSubtype, TypeMediaMessage, wa } from '@api/types/wa.types';
import { CacheEngine } from '@cache/cacheengine';
import {
  CacheConf,
  Chatwoot,
  ConfigService,
  configService,
  ConfigSessionPhone,
  Database,
  Log,
  Openai,
  ProviderSession,
  QrCode,
  S3,
} from '@config/env.config';
import { BadRequestException, InternalServerErrorException, NotFoundException } from '@exceptions';
import ffmpegPath from '@ffmpeg-installer/ffmpeg';
import { Boom } from '@hapi/boom';
import { createId as cuid } from '@paralleldrive/cuid2';
import { Instance, Message } from '@prisma/client';
import { createJid } from '@utils/createJid';
import { fetchLatestWaWebVersion } from '@utils/fetchLatestWaWebVersion';
import { makeProxyAgent } from '@utils/makeProxyAgent';
import { getOnWhatsappCache, saveOnWhatsappCache } from '@utils/onWhatsappCache';
import { status } from '@utils/renderStatus';
import useMultiFileAuthStatePrisma from '@utils/use-multi-file-auth-state-prisma';
import { AuthStateProvider } from '@utils/use-multi-file-auth-state-provider-files';
import { useMultiFileAuthStateRedisDb } from '@utils/use-multi-file-auth-state-redis-db';
import axios from 'axios';
import makeWASocket, {
  AnyMessageContent,
  BufferedEventData,
  BufferJSON,
  CacheStore,
  CatalogCollection,
  Chat,
  ConnectionState,
  Contact,
  delay,
  DisconnectReason,
  downloadMediaMessage,
  generateWAMessageFromContent,
  getAggregateVotesInPollMessage,
  GetCatalogOptions,
  getContentType,
  getDevice,
  GroupMetadata,
  isJidBroadcast,
  isJidGroup,
  isJidNewsletter,
  isJidUser,
  makeCacheableSignalKeyStore,
  MessageUpsertType,
  MessageUserReceiptUpdate,
  MiscMessageGenerationOptions,
  ParticipantAction,
  prepareWAMessageMedia,
  Product,
  proto,
  UserFacingSocketConfig,
  WABrowserDescription,
  WAMediaUpload,
  WAMessage,
  WAMessageUpdate,
  WAPresence,
  WASocket,
} from 'baileys';
import { Label } from 'baileys/lib/Types/Label';
import { LabelAssociation } from 'baileys/lib/Types/LabelAssociation';
import { spawn } from 'child_process';
import { isArray, isBase64, isURL } from 'class-validator';
import { randomBytes } from 'crypto';
import EventEmitter2 from 'eventemitter2';
import ffmpeg from 'fluent-ffmpeg';
import FormData from 'form-data';
import Long from 'long';
import mimeTypes from 'mime-types';
import NodeCache from 'node-cache';
import cron from 'node-cron';
import { release } from 'os';
import { join } from 'path';
import P from 'pino';
import qrcode, { QRCodeToDataURLOptions } from 'qrcode';
import qrcodeTerminal from 'qrcode-terminal';
import sharp from 'sharp';
import { PassThrough, Readable } from 'stream';
import { v4 } from 'uuid';

import { useVoiceCallsBaileys } from './voiceCalls/useVoiceCallsBaileys';

const groupMetadataCache = new CacheService(new CacheEngine(configService, 'groups').getEngine());

// Adicione a função getVideoDuration no início do arquivo
async function getVideoDuration(input: Buffer | string | Readable): Promise<number> {
  const MediaInfoFactory = (await import('mediainfo.js')).default;
  const mediainfo = await MediaInfoFactory({ format: 'JSON' });

  let fileSize: number;
  let readChunk: (size: number, offset: number) => Promise<Buffer>;

  if (Buffer.isBuffer(input)) {
    fileSize = input.length;
    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return input.slice(offset, offset + size);
    };
  } else if (typeof input === 'string') {
    const fs = await import('fs');
    const stat = await fs.promises.stat(input);
    fileSize = stat.size;
    const fd = await fs.promises.open(input, 'r');

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      const buffer = Buffer.alloc(size);
      await fd.read(buffer, 0, size, offset);
      return buffer;
    };

    try {
      const result = await mediainfo.analyzeData(() => fileSize, readChunk);
      const jsonResult = JSON.parse(result);

      const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
      const duration = generalTrack.Duration;

      return Math.round(parseFloat(duration));
    } finally {
      await fd.close();
    }
  } else if (input instanceof Readable) {
    const chunks: Buffer[] = [];
    for await (const chunk of input) {
      chunks.push(chunk);
    }
    const data = Buffer.concat(chunks);
    fileSize = data.length;

    readChunk = async (size: number, offset: number): Promise<Buffer> => {
      return data.slice(offset, offset + size);
    };
  } else {
    throw new Error('Tipo de entrada não suportado');
  }

  const result = await mediainfo.analyzeData(() => fileSize, readChunk);
  const jsonResult = JSON.parse(result);

  const generalTrack = jsonResult.media.track.find((t: any) => t['@type'] === 'General');
  const duration = generalTrack.Duration;

  return Math.round(parseFloat(duration));
}

export class BaileysStartupService extends ChannelStartupService {
  constructor(
      public readonly configService: ConfigService,
      public readonly eventEmitter: EventEmitter2,
      public readonly prismaRepository: PrismaRepository,
      public readonly cache: CacheService,
      public readonly chatwootCache: CacheService,
      public readonly baileysCache: CacheService,
      private readonly providerFiles: ProviderFiles,
  ) {
    super(configService, eventEmitter, prismaRepository, chatwootCache);
    this.instance.qrcode = { count: 0 };

    this.authStateProvider = new AuthStateProvider(this.providerFiles);
  }

  private authStateProvider: AuthStateProvider;
  private readonly msgRetryCounterCache: CacheStore = new NodeCache();
  private readonly userDevicesCache: CacheStore = new NodeCache({ stdTTL: 300000, useClones: false });
  private endSession = false;
  private logBaileys = this.configService.get<Log>('LOG').BAILEYS;

  public stateConnection: wa.StateConnection = { state: 'close' };

  public phoneNumber: string;

  public get connectionStatus() {
    return this.stateConnection;
  }

  public async logoutInstance() {
    await this.client?.logout('Log out instance: ' + this.instanceName);

    this.client?.ws?.close();

    const sessionExists = await this.prismaRepository.session.findFirst({ where: { sessionId: this.instanceId } });
    if (sessionExists) {
      await this.prismaRepository.session.delete({ where: { sessionId: this.instanceId } });
    }
  }

  public async getProfileName() {
    let profileName = this.client.user?.name ?? this.client.user?.verifiedName;
    if (!profileName) {
      const data = await this.prismaRepository.session.findUnique({ where: { sessionId: this.instanceId } });

      if (data) {
        const creds = JSON.parse(JSON.stringify(data.creds), BufferJSON.reviver);
        profileName = creds.me?.name || creds.me?.verifiedName;
      }
    }

    return profileName;
  }

  public async getProfileStatus() {
    const status = await this.client.fetchStatus(this.instance.wuid);

    return status[0]?.status;
  }

  public get profilePictureUrl() {
    return this.instance.profilePictureUrl;
  }

  public get qrCode(): wa.QrCode {
    return {
      pairingCode: this.instance.qrcode?.pairingCode,
      code: this.instance.qrcode?.code,
      base64: this.instance.qrcode?.base64,
      count: this.instance.qrcode?.count,
    };
  }

  private async connectionUpdate({ qr, connection, lastDisconnect }: Partial<ConnectionState>) {
    this.logger.info(`Received connection update: connection=${connection}, qr=${qr ? 'exists' : 'no'}`);
    if (qr) {
      if (this.instance.qrcode.count === this.configService.get<QrCode>('QRCODE').LIMIT) {
        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          message: 'QR code limit reached, please login again',
          statusCode: DisconnectReason.badSession,
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
              Events.QRCODE_UPDATED,
              { instanceName: this.instance.name, instanceId: this.instanceId },
              { message: 'QR code limit reached, please login again', statusCode: DisconnectReason.badSession },
          );
        }

        this.sendDataWebhook(Events.CONNECTION_UPDATE, {
          instance: this.instance.name,
          state: 'refused',
          statusReason: DisconnectReason.connectionClosed,
          wuid: this.instance.wuid,
          profileName: await this.getProfileName(),
          profilePictureUrl: this.instance.profilePictureUrl,
        });

        this.endSession = true;

        return this.eventEmitter.emit('no.connection', this.instance.name);
      }

      this.instance.qrcode.count++;

      const color = this.configService.get<QrCode>('QRCODE').COLOR;

      const optsQrcode: QRCodeToDataURLOptions = {
        margin: 3,
        scale: 4,
        errorCorrectionLevel: 'H',
        color: { light: '#ffffff', dark: color },
      };

      if (this.phoneNumber) {
        await delay(1000);
        this.instance.qrcode.pairingCode = await this.client.requestPairingCode(this.phoneNumber);
      } else {
        this.instance.qrcode.pairingCode = null;
      }

      qrcode.toDataURL(qr, optsQrcode, (error, base64) => {
        if (error) {
          this.logger.error('Qrcode generate failed:' + error.toString());
          return;
        }

        this.instance.qrcode.base64 = base64;
        this.instance.qrcode.code = qr;

        this.sendDataWebhook(Events.QRCODE_UPDATED, {
          qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
              Events.QRCODE_UPDATED,
              { instanceName: this.instance.name, instanceId: this.instanceId },
              {
                qrcode: { instance: this.instance.name, pairingCode: this.instance.qrcode.pairingCode, code: qr, base64 },
              },
          );
        }
      });

      qrcodeTerminal.generate(qr, { small: true }, (qrcode) =>
          this.logger.log(
              `\n{ instance: ${this.instance.name} pairingCode: ${this.instance.qrcode.pairingCode}, qrcodeCount: ${this.instance.qrcode.count} }\n` +
              qrcode,
          ),
      );

      await this.prismaRepository.instance.update({
        where: { id: this.instanceId },
        data: { connectionStatus: 'connecting' },
      });
    }

    if (connection) {
      this.stateConnection = {
        state: connection,
        statusReason: (lastDisconnect?.error as Boom)?.output?.statusCode ?? 200,
      };
    }

    if (connection === 'close') {
      const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
      const codesToNotReconnect = [DisconnectReason.loggedOut, DisconnectReason.forbidden, 402, 406];
      const shouldReconnect = !codesToNotReconnect.includes(statusCode);
      if (shouldReconnect) {
        await this.connectToWhatsapp(this.phoneNumber);
      } else {
        this.sendDataWebhook(Events.STATUS_INSTANCE, {
          instance: this.instance.name,
          status: 'closed',
          disconnectionAt: new Date(),
          disconnectionReasonCode: statusCode,
          disconnectionObject: JSON.stringify(lastDisconnect),
        });

        await this.prismaRepository.instance.update({
          where: { id: this.instanceId },
          data: {
            connectionStatus: 'close',
            disconnectionAt: new Date(),
            disconnectionReasonCode: statusCode,
            disconnectionObject: JSON.stringify(lastDisconnect),
          },
        });

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
          this.chatwootService.eventWhatsapp(
              Events.STATUS_INSTANCE,
              { instanceName: this.instance.name, instanceId: this.instance.id },
              { instance: this.instance.name, status: 'closed' },
          );
        }

        this.eventEmitter.emit('logout.instance', this.instance.name, 'inner');
        this.client?.ws?.close();
        this.client.end(new Error('Close connection'));

        this.sendDataWebhook(Events.CONNECTION_UPDATE, { instance: this.instance.name, ...this.stateConnection });
      }
    }

    if (connection === 'open') {
      this.instance.wuid = this.client.user.id.replace(/:\d+/, '');
      try {
        const profilePic = await this.profilePicture(this.instance.wuid);
        this.instance.profilePictureUrl = profilePic.profilePictureUrl;
      } catch (error) {
        this.instance.profilePictureUrl = null;
      }
      const formattedWuid = this.instance.wuid.split('@')[0].padEnd(30, ' ');
      const formattedName = this.instance.name;
      this.logger.info(
          `
        ┌──────────────────────────────┐
        │    CONNECTED TO WHATSAPP     │
        └──────────────────────────────┘`.replace(/^ +/gm, '  '),
      );
      this.logger.info(
          `
        wuid: ${formattedWuid}
        name: ${formattedName}
      `,
      );

      await this.prismaRepository.instance.update({
        where: { id: this.instanceId },
        data: {
          ownerJid: this.instance.wuid,
          profileName: (await this.getProfileName()) as string,
          profilePicUrl: this.instance.profilePictureUrl,
          connectionStatus: 'open',
        },
      });

      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
        this.chatwootService.eventWhatsapp(
            Events.CONNECTION_UPDATE,
            { instanceName: this.instance.name, instanceId: this.instance.id },
            { instance: this.instance.name, status: 'open' },
        );
        this.syncChatwootLostMessages();
      }

      this.sendDataWebhook(Events.CONNECTION_UPDATE, {
        instance: this.instance.name,
        wuid: this.instance.wuid,
        profileName: await this.getProfileName(),
        profilePictureUrl: this.instance.profilePictureUrl,
        ...this.stateConnection,
      });
    }

    if (connection === 'connecting') {
      this.sendDataWebhook(Events.CONNECTION_UPDATE, { instance: this.instance.name, ...this.stateConnection });
    }
  }

  private async getMessage(key: proto.IMessageKey, full = false) {
    try {
      const webMessageInfo = (await this.prismaRepository.message.findMany({
        where: { instanceId: this.instanceId, key: { path: ['id'], equals: key.id } },
      })) as unknown as proto.IWebMessageInfo[];
      if (full) {
        return webMessageInfo[0];
      }
      if (webMessageInfo[0].message?.pollCreationMessage) {
        const messageSecretBase64 = webMessageInfo[0].message?.messageContextInfo?.messageSecret;

        if (typeof messageSecretBase64 === 'string') {
          const messageSecret = Buffer.from(messageSecretBase64, 'base64');

          const msg = {
            messageContextInfo: { messageSecret },
            pollCreationMessage: webMessageInfo[0].message?.pollCreationMessage,
          };

          return msg;
        }
      }

      return webMessageInfo[0].message;
    } catch (error) {
      return { conversation: '' };
    }
  }

  private async defineAuthState() {
    const db = this.configService.get<Database>('DATABASE');
    const cache = this.configService.get<CacheConf>('CACHE');

    const provider = this.configService.get<ProviderSession>('PROVIDER');

    if (provider?.ENABLED) {
      return await this.authStateProvider.authStateProvider(this.instance.id);
    }

    if (cache?.REDIS.ENABLED && cache?.REDIS.SAVE_INSTANCES) {
      this.logger.info('Redis enabled');
      return await useMultiFileAuthStateRedisDb(this.instance.id, this.cache);
    }

    if (db.SAVE_DATA.INSTANCE) {
      return await useMultiFileAuthStatePrisma(this.instance.id, this.cache);
    }
  }

  private async createClient(number?: string): Promise<WASocket> {
    this.instance.authState = await this.defineAuthState();

    const session = this.configService.get<ConfigSessionPhone>('CONFIG_SESSION_PHONE');

    let browserOptions = {};

    if (number || this.phoneNumber) {
      this.phoneNumber = number;

      this.logger.info(`Phone number: ${number}`);
    } else {
      const browser: WABrowserDescription = [session.CLIENT, session.NAME, release()];
      browserOptions = { browser };

      this.logger.info(`Browser: ${browser}`);
    }

    let version;
    let log;

    if (session.VERSION) {
      version = session.VERSION.split('.');
      log = `Baileys version env: ${version}`;
    } else {
      const baileysVersion = await fetchLatestWaWebVersion({});
      version = baileysVersion.version;
      log = `Baileys version: ${version}`;
    }

    this.logger.info(log);

    this.logger.info(`Group Ignore: ${this.localSettings.groupsIgnore}`);

    let options;

    if (this.localProxy?.enabled) {
      this.logger.info('Proxy enabled: ' + this.localProxy?.host);

      if (this.localProxy?.host?.includes('proxyscrape')) {
        try {
          const response = await axios.get(this.localProxy?.host);
          const text = response.data;
          const proxyUrls = text.split('\r\n');
          const rand = Math.floor(Math.random() * Math.floor(proxyUrls.length));
          const proxyUrl = 'http://' + proxyUrls[rand];
          options = { agent: makeProxyAgent(proxyUrl), fetchAgent: makeProxyAgent(proxyUrl) };
        } catch (error) {
          this.localProxy.enabled = false;
        }
      } else {
        options = {
          agent: makeProxyAgent({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
          fetchAgent: makeProxyAgent({
            host: this.localProxy.host,
            port: this.localProxy.port,
            protocol: this.localProxy.protocol,
            username: this.localProxy.username,
            password: this.localProxy.password,
          }),
        };
      }
    }

    const socketConfig: UserFacingSocketConfig = {
      ...options,
      version,
      logger: P({ level: this.logBaileys }),
      printQRInTerminal: false,
      auth: {
        creds: this.instance.authState.state.creds,
        keys: makeCacheableSignalKeyStore(this.instance.authState.state.keys, P({ level: 'error' }) as any),
      },
      msgRetryCounterCache: this.msgRetryCounterCache,
      generateHighQualityLinkPreview: true,
      getMessage: async (key) => (await this.getMessage(key)) as Promise<proto.IMessage>,
      ...browserOptions,
      markOnlineOnConnect: this.localSettings.alwaysOnline,
      retryRequestDelayMs: 350,
      maxMsgRetryCount: 4,
      fireInitQueries: true,
      connectTimeoutMs: 30_000,
      keepAliveIntervalMs: 30_000,
      qrTimeout: 45_000,
      emitOwnEvents: false,
      shouldIgnoreJid: (jid) => {
        if (this.localSettings.syncFullHistory && isJidGroup(jid)) {
          return false;
        }

        const isGroupJid = this.localSettings.groupsIgnore && isJidGroup(jid);
        const isBroadcast = !this.localSettings.readStatus && isJidBroadcast(jid);
        const isNewsletter = isJidNewsletter(jid);

        return isGroupJid || isBroadcast || isNewsletter;
      },
      syncFullHistory: this.localSettings.syncFullHistory,
      shouldSyncHistoryMessage: (msg: proto.Message.IHistorySyncNotification) => {
        return this.historySyncNotification(msg);
      },
      cachedGroupMetadata: this.getGroupMetadataCache,
      userDevicesCache: this.userDevicesCache,
      transactionOpts: { maxCommitRetries: 10, delayBetweenTriesMs: 3000 },
      patchMessageBeforeSending(message) {
        if (
            message.deviceSentMessage?.message?.listMessage?.listType === proto.Message.ListMessage.ListType.PRODUCT_LIST
        ) {
          message = JSON.parse(JSON.stringify(message));

          message.deviceSentMessage.message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        if (message.listMessage?.listType == proto.Message.ListMessage.ListType.PRODUCT_LIST) {
          message = JSON.parse(JSON.stringify(message));

          message.listMessage.listType = proto.Message.ListMessage.ListType.SINGLE_SELECT;
        }

        return message;
      },
    };

    this.endSession = false;

    this.client = makeWASocket(socketConfig);

    if (this.localSettings.wavoipToken && this.localSettings.wavoipToken.length > 0) {
      useVoiceCallsBaileys(this.localSettings.wavoipToken, this.client, this.connectionStatus.state as any, true);
    }

    this.eventHandler();

    this.client.ws.on('CB:call', (packet) => {
      this.logger.info(`Received CB:call event: ${JSON.stringify(packet)}`);
      console.log('CB:call', packet);
      const payload = { event: 'CB:call', packet: packet };
      this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
    });

    this.client.ws.on('CB:ack,class:call', (packet) => {
      this.logger.info(`Received CB:ack,class:call event: ${JSON.stringify(packet)}`);
      console.log('CB:ack,class:call', packet);
      const payload = { event: 'CB:ack,class:call', packet: packet };
      this.sendDataWebhook(Events.CALL, payload, true, ['websocket']);
    });

    this.phoneNumber = number;

    return this.client;
  }

  public async connectToWhatsapp(number?: string): Promise<WASocket> {
    try {
      this.loadChatwoot();
      this.loadSettings();
      this.loadWebhook();
      this.loadProxy();

      return await this.createClient(number);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  public async reloadConnection(): Promise<WASocket> {
    try {
      return await this.createClient(this.phoneNumber);
    } catch (error) {
      this.logger.error(error);
      throw new InternalServerErrorException(error?.toString());
    }
  }

  private readonly chatHandle = {
    'chats.upsert': async (chats: Chat[]) => {
      this.logger.info(`Received chats.upsert event with ${chats.length} chats.`);
      const existingChatIds = await this.prismaRepository.chat.findMany({
        where: { instanceId: this.instanceId },
        select: { remoteJid: true },
      });

      const existingChatIdSet = new Set(existingChatIds.map((chat) => chat.remoteJid));

      const chatsToInsert = chats
          .filter((chat) => !existingChatIdSet?.has(chat.id))
          .map((chat) => ({
            remoteJid: chat.id,
            instanceId: this.instanceId,
            name: chat.name,
            unreadMessages: chat.unreadCount !== undefined ? chat.unreadCount : 0,
          }));

      this.sendDataWebhook(Events.CHATS_UPSERT, chatsToInsert);

      if (chatsToInsert.length > 0) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS)
          await this.prismaRepository.chat.createMany({ data: chatsToInsert, skipDuplicates: true });
      }
    },

    'chats.update': async (
        chats: Partial<
            proto.IConversation & { lastMessageRecvTimestamp?: number } & {
          conditional: (bufferedData: BufferedEventData) => boolean;
        }
        >[],
    ) => {
      this.logger.info(`Received chats.update event with ${chats.length} chats.`);
      const chatsRaw = chats.map((chat) => {
        return { remoteJid: chat.id, instanceId: this.instanceId };
      });

      this.sendDataWebhook(Events.CHATS_UPDATE, chatsRaw);

      for (const chat of chats) {
        await this.prismaRepository.chat.updateMany({
          where: { instanceId: this.instanceId, remoteJid: chat.id, name: chat.name },
          data: { remoteJid: chat.id },
        });
      }
    },

    'chats.delete': async (chats: string[]) => {
      this.logger.info(`Received chats.delete event with ${chats.length} chats.`);
      chats.forEach(
          async (chat) =>
              await this.prismaRepository.chat.deleteMany({ where: { instanceId: this.instanceId, remoteJid: chat } }),
      );

      this.sendDataWebhook(Events.CHATS_DELETE, [...chats]);
    },
  };

  private readonly contactHandle = {
    'contacts.upsert': async (contacts: Contact[]) => {
      this.logger.info(`Received contacts.upsert event with ${contacts.length} contacts.`);
      try {
        const contactsRaw: any = contacts.map((contact) => ({
          remoteJid: contact.id,
          pushName: contact?.name || contact?.verifiedName || contact.id.split('@')[0],
          profilePicUrl: null,
          instanceId: this.instanceId,
        }));

        if (contactsRaw.length > 0) {
          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactsRaw);

          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.createMany({ data: contactsRaw, skipDuplicates: true });

          const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
          if (usersContacts) {
            await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
          }
        }

        if (
            this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
            this.localChatwoot?.enabled &&
            this.localChatwoot.importContacts &&
            contactsRaw.length
        ) {
          this.chatwootService.addHistoryContacts(
              { instanceName: this.instance.name, instanceId: this.instance.id },
              contactsRaw,
          );
          chatwootImport.importHistoryContacts(
              { instanceName: this.instance.name, instanceId: this.instance.id },
              this.localChatwoot,
          );
        }

        const updatedContacts = await Promise.all(
            contacts.map(async (contact) => ({
              remoteJid: contact.id,
              pushName: contact?.name || contact?.verifiedName || contact.id.split('@')[0],
              profilePicUrl: (await this.profilePicture(contact.id)).profilePictureUrl,
              instanceId: this.instanceId,
            })),
        );

        if (updatedContacts.length > 0) {
          const usersContacts = updatedContacts.filter((c) => c.remoteJid.includes('@s.whatsapp'));
          if (usersContacts) {
            await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
          }

          this.sendDataWebhook(Events.CONTACTS_UPDATE, updatedContacts);
          await Promise.all(
              updatedContacts.map(async (contact) => {
                const update = this.prismaRepository.contact.updateMany({
                  where: { remoteJid: contact.remoteJid, instanceId: this.instanceId },
                  data: { profilePicUrl: contact.profilePicUrl },
                });

                if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
                  const instance = { instanceName: this.instance.name, instanceId: this.instance.id };

                  const findParticipant = await this.chatwootService.findContact(
                      instance,
                      contact.remoteJid.split('@')[0],
                  );

                  if (!findParticipant) {
                    return;
                  }

                  this.chatwootService.updateContact(instance, findParticipant.id, {
                    name: contact.pushName,
                    avatar_url: contact.profilePicUrl,
                  });
                }

                return update;
              }),
          );
        }
      } catch (error) {
        console.error(error);
        this.logger.error(`Error: ${error.message}`);
      }
    },

    'contacts.update': async (contacts: Partial<Contact>[]) => {
      this.logger.info(`Received contacts.update event with ${contacts.length} contacts.`);
      const contactsRaw: { remoteJid: string; pushName?: string; profilePicUrl?: string; instanceId: string }[] = [];
      for await (const contact of contacts) {
        contactsRaw.push({
          remoteJid: contact.id,
          pushName: contact?.name ?? contact?.verifiedName,
          profilePicUrl: (await this.profilePicture(contact.id)).profilePicUrl,
          instanceId: this.instanceId,
        });
      }

      this.sendDataWebhook(Events.CONTACTS_UPDATE, contactsRaw);

      const updateTransactions = contactsRaw.map((contact) =>
          this.prismaRepository.contact.upsert({
            where: { remoteJid_instanceId: { remoteJid: contact.remoteJid, instanceId: contact.instanceId } },
            create: contact,
            update: contact,
          }),
      );
      await this.prismaRepository.$transaction(updateTransactions);

      const usersContacts = contactsRaw.filter((c) => c.remoteJid.includes('@s.whatsapp'));
      if (usersContacts) {
        await saveOnWhatsappCache(usersContacts.map((c) => ({ remoteJid: c.remoteJid })));
      }
    },
  };

  private readonly messageHandle = {
    'messaging-history.set': async ({
                                      messages,
                                      chats,
                                      contacts,
                                      isLatest,
                                      progress,
                                      syncType,
                                    }: {
      chats: Chat[];
      contacts: Contact[];
      messages: proto.IWebMessageInfo[];
      isLatest?: boolean;
      progress?: number;
      syncType?: proto.HistorySync.HistorySyncType;
    }) => {
      this.logger.info(`Received messaging-history.set event. Sync type: ${syncType}, messages: ${messages.length}`);
      try {
        if (syncType === proto.HistorySync.HistorySyncType.ON_DEMAND) {
          console.log('received on-demand history sync, messages=', messages);
        }
        console.log(
            `recv ${chats.length} chats, ${contacts.length} contacts, ${messages.length} msgs (is latest: ${isLatest}, progress: ${progress}%), type: ${syncType}`,
        );

        const instance: InstanceDto = { instanceName: this.instance.name };

        let timestampLimitToImport = null;

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
          const daysLimitToImport = this.localChatwoot?.enabled ? this.localChatwoot.daysLimitImportMessages : 1000;

          const date = new Date();
          timestampLimitToImport = new Date(date.setDate(date.getDate() - daysLimitToImport)).getTime() / 1000;

          const maxBatchTimestamp = Math.max(...messages.map((message) => message.messageTimestamp as number));

          const processBatch = maxBatchTimestamp >= timestampLimitToImport;

          if (!processBatch) {
            return;
          }
        }

        const contactsMap = new Map();

        for (const contact of contacts) {
          if (contact.id && (contact.notify || contact.name)) {
            contactsMap.set(contact.id, { name: contact.name ?? contact.notify, jid: contact.id });
          }
        }

        const chatsRaw: { remoteJid: string; instanceId: string; name?: string }[] = [];
        const chatsRepository = new Set(
            (await this.prismaRepository.chat.findMany({ where: { instanceId: this.instanceId } })).map(
                (chat) => chat.remoteJid,
            ),
        );

        for (const chat of chats) {
          if (chatsRepository?.has(chat.id)) {
            continue;
          }

          chatsRaw.push({ remoteJid: chat.id, instanceId: this.instanceId, name: chat.name });
        }

        this.sendDataWebhook(Events.CHATS_SET, chatsRaw);

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await this.prismaRepository.chat.createMany({ data: chatsRaw, skipDuplicates: true });
        }

        const messagesRaw: any[] = [];

        const messagesRepository: Set<string> = new Set(
            chatwootImport.getRepositoryMessagesCache(instance) ??
            (
                await this.prismaRepository.message.findMany({
                  select: { key: true },
                  where: { instanceId: this.instanceId },
                })
            ).map((message) => {
              const key = message.key as { id: string };

              return key.id;
            }),
        );

        if (chatwootImport.getRepositoryMessagesCache(instance) === null) {
          chatwootImport.setRepositoryMessagesCache(instance, messagesRepository);
        }

        for (const m of messages) {
          if (!m.message || !m.key || !m.messageTimestamp) {
            continue;
          }

          if (Long.isLong(m?.messageTimestamp)) {
            m.messageTimestamp = m.messageTimestamp?.toNumber();
          }

          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED) {
            if (m.messageTimestamp <= timestampLimitToImport) {
              continue;
            }
          }

          if (messagesRepository?.has(m.key.id)) {
            continue;
          }

          if (!m.pushName && !m.key.fromMe) {
            const participantJid = m.participant || m.key.participant || m.key.remoteJid;
            if (participantJid && contactsMap.has(participantJid)) {
              m.pushName = contactsMap.get(participantJid).name;
            } else if (participantJid) {
              m.pushName = participantJid.split('@')[0];
            }
          }

          messagesRaw.push(this.prepareMessage(m));
        }

        this.sendDataWebhook(Events.MESSAGES_SET, [...messagesRaw]);

        if (this.configService.get<Database>('DATABASE').SAVE_DATA.HISTORIC) {
          await this.prismaRepository.message.createMany({ data: messagesRaw, skipDuplicates: true });
        }

        if (
            this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
            this.localChatwoot?.enabled &&
            this.localChatwoot.importMessages &&
            messagesRaw.length > 0
        ) {
          this.chatwootService.addHistoryMessages(
              instance,
              messagesRaw.filter((msg) => !chatwootImport.isIgnorePhoneNumber(msg.key?.remoteJid)),
          );
        }

        await this.contactHandle['contacts.upsert'](
            contacts.filter((c) => !!c.notify || !!c.name).map((c) => ({ id: c.id, name: c.name ?? c.notify })),
        );

        contacts = undefined;
        messages = undefined;
        chats = undefined;
      } catch (error) {
        this.logger.error(error);
      }
    },

    'messages.upsert': async (
        { messages, type, requestId }: { messages: proto.IWebMessageInfo[]; type: MessageUpsertType; requestId?: string },
        settings: any,
    ) => {
      this.logger.info(`Received messages.upsert event (type: ${type}) with ${messages.length} messages.`);
      try {
        for (const received of messages) {
          if (received.message?.conversation || received.message?.extendedTextMessage?.text) {
            const text = received.message?.conversation || received.message?.extendedTextMessage?.text;

            if (text == 'requestPlaceholder' && !requestId) {
              const messageId = await this.client.requestPlaceholderResend(received.key);

              console.log('requested placeholder resync, id=', messageId);
            } else if (requestId) {
              console.log('Message received from phone, id=', requestId, received);
            }

            if (text == 'onDemandHistSync') {
              const messageId = await this.client.fetchMessageHistory(50, received.key, received.messageTimestamp!);
              console.log('requested on-demand sync, id=', messageId);
            }
          }

          const editedMessage =
              received?.message?.protocolMessage || received?.message?.editedMessage?.message?.protocolMessage;

          if (received.message?.protocolMessage?.editedMessage && editedMessage) {
            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
              this.chatwootService.eventWhatsapp(
                  'messages.edit',
                  { instanceName: this.instance.name, instanceId: this.instance.id },
                  editedMessage,
              );

            await this.sendDataWebhook(Events.MESSAGES_EDITED, editedMessage);
            const oldMessage = await this.getMessage(editedMessage.key, true);
            if ((oldMessage as any)?.id) {
              const editedMessageTimestamp = Long.isLong(editedMessage?.timestampMs)
                  ? Math.floor(editedMessage.timestampMs.toNumber() / 1000)
                  : Math.floor((editedMessage.timestampMs as number) / 1000);

              await this.prismaRepository.message.update({
                where: { id: (oldMessage as any).id },
                data: {
                  message: editedMessage.editedMessage as any,
                  messageTimestamp: editedMessageTimestamp,
                  status: 'EDITED',
                },
              });
              await this.prismaRepository.messageUpdate.create({
                data: {
                  fromMe: editedMessage.key.fromMe,
                  keyId: editedMessage.key.id,
                  remoteJid: editedMessage.key.remoteJid,
                  status: 'EDITED',
                  instanceId: this.instanceId,
                  messageId: (oldMessage as any).id,
                },
              });
            }
          }

          const messageKey = `${this.instance.id}_${received.key.id}`;
          const cached = await this.baileysCache.get(messageKey);

          if (cached && !editedMessage) {
            this.logger.info(`Message duplicated ignored: ${received.key.id}`);
            continue;
          }

          await this.baileysCache.set(messageKey, true, 5 * 60);

          if (
              (type !== 'notify' && type !== 'append') ||
              received.message?.protocolMessage ||
              !received?.message
          ) {
            continue;
          }

          if (Long.isLong(received.messageTimestamp)) {
            received.messageTimestamp = received.messageTimestamp?.toNumber();
          }

          if (settings?.groupsIgnore && received.key.remoteJid.includes('@g.us')) {
            continue;
          }

          const existingChat = await this.prismaRepository.chat.findFirst({
            where: { instanceId: this.instanceId, remoteJid: received.key.remoteJid },
            select: { id: true, name: true },
          });

          if (
              existingChat &&
              received.pushName &&
              existingChat.name !== received.pushName &&
              received.pushName.trim().length > 0 &&
              !received.key.fromMe &&
              !received.key.remoteJid.includes('@g.us')
          ) {
            this.sendDataWebhook(Events.CHATS_UPSERT, [{ ...existingChat, name: received.pushName }]);
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS) {
              try {
                await this.prismaRepository.chat.update({ where: { id: existingChat.id }, data: { name: received.pushName }, });
              } catch (error) {
                console.log(`Chat insert record ignored: ${received.key.remoteJid} - ${this.instanceId}`);
              }
            }
          }

          const messageRaw = this.prepareMessage(received);
          const isMedia = received?.message?.imageMessage || received?.message?.videoMessage || received?.message?.stickerMessage || received?.message?.documentMessage || received?.message?.documentWithCaptionMessage || received?.message?.ptvMessage || received?.message?.audioMessage;
          if (this.localSettings.readMessages && received.key.id !== 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }
          if (this.localSettings.readStatus && received.key.id === 'status@broadcast') {
            await this.client.readMessages([received.key]);
          }
          if (
              this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
              this.localChatwoot?.enabled &&
              !received.key.id.includes('@broadcast')
          ) {
            const chatwootSentMessage = await this.chatwootService.eventWhatsapp(
                Events.MESSAGES_UPSERT,
                { instanceName: this.instance.name, instanceId: this.instance.id },
                messageRaw,
            );
            if (chatwootSentMessage?.id) {
              messageRaw.chatwootMessageId = chatwootSentMessage.id;
              messageRaw.chatwootInboxId = chatwootSentMessage.inbox_id;
              messageRaw.chatwootConversationId = chatwootSentMessage.conversation_id;
            }
          }
          if (this.configService.get<Openai>('OPENAI').ENABLED && received?.message?.audioMessage) {
            const openAiDefaultSettings = await this.prismaRepository.openaiSetting.findFirst({ where: { instanceId: this.instanceId }, include: { OpenaiCreds: true }, });
            if (openAiDefaultSettings && openAiDefaultSettings.openaiCredsId && openAiDefaultSettings.speechToText) {
              messageRaw.message.speechToText = `[audio] ${await this.openaiService.speechToText(received, this)}`;
            }
          }
          if (this.configService.get<Database>('DATABASE').SAVE_DATA.NEW_MESSAGE) {
            const msg = await this.prismaRepository.message.create({ data: messageRaw });
            const { remoteJid } = received.key;
            const timestamp = msg.messageTimestamp;
            const fromMe = received.key.fromMe.toString();
            const messageKey = `${remoteJid}_${timestamp}_${fromMe}`;
            const cachedTimestamp = await this.baileysCache.get(messageKey);
            if (!cachedTimestamp) {
              if (!received.key.fromMe) {
                if (msg.status === status[3]) {
                  this.logger.log(`Update not read messages ${remoteJid}`);
                  await this.updateChatUnreadMessages(remoteJid);
                } else if (msg.status === status[4]) {
                  this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                  await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
                }
              } else {
                // is send message by me
                this.logger.log(`Update readed messages ${remoteJid} - ${timestamp}`);
                await this.updateMessagesReadedByTimestamp(remoteJid, timestamp);
              }
              await this.baileysCache.set(messageKey, true, 5 * 60);
            } else {
              this.logger.info(`Update readed messages duplicated ignored [avoid deadlock]: ${messageKey}`);
            }
            if (isMedia) {
              if (this.configService.get<S3>('S3').ENABLE) {
                try {
                  const message: any = received;
                  const media = await this.getBase64FromMediaMessage({ message }, true);
                  const { buffer, mediaType, fileName, size } = media;
                  const mimetype = mimeTypes.lookup(fileName).toString();
                  const fullName = join(
                      `${this.instance.id}`,
                      received.key.remoteJid,
                      mediaType,
                      `${Date.now()}_${fileName}`,
                  );
                  await s3Service.uploadFile(fullName, buffer, size.fileLength?.low, { 'Content-Type': mimetype });
                  await this.prismaRepository.media.create({ data: { messageId: msg.id, instanceId: this.instanceId, type: mediaType, fileName: fullName, mimetype, }, });
                  const mediaUrl = await s3Service.getObjectUrl(fullName);
                  messageRaw.message.mediaUrl = mediaUrl;
                  await this.prismaRepository.message.update({ where: { id: msg.id }, data: messageRaw });
                } catch (error) {
                  this.logger.error(['Error on upload file to minio', error?.message, error?.stack]);
                }
              }
            }
          }
          if (this.localWebhook.enabled) {
            if (isMedia && this.localWebhook.webhookBase64) {
              try {
                const buffer = await downloadMediaMessage(
                    { key: received.key, message: received?.message },
                    'buffer',
                    {},
                    { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                );
                if (buffer) {
                  messageRaw.message.base64 = buffer.toString('base64');
                } else {
                  // retry to download media
                  const buffer = await downloadMediaMessage(
                      { key: received.key, message: received?.message },
                      'buffer',
                      {},
                      { logger: P({ level: 'error' }) as any, reuploadRequest: this.client.updateMediaMessage },
                  );
                  if (buffer) {
                    messageRaw.message.base64 = buffer.toString('base64');
                  }
                }
              } catch (error) {
                this.logger.error(['Error converting media to base64', error?.message]);
              }
            }
          }
          this.logger.log(messageRaw);
          this.sendDataWebhook(Events.MESSAGES_UPSERT, messageRaw);
          await chatbotController.emit({ instance: { instanceName: this.instance.name, instanceId: this.instanceId }, remoteJid: messageRaw.key.remoteJid, msg: messageRaw, pushName: messageRaw.pushName, });
          const contact = await this.prismaRepository.contact.findFirst({ where: { remoteJid: received.key.remoteJid, instanceId: this.instanceId }, });
          const contactRaw: { remoteJid: string; pushName: string; profilePicUrl?: string; instanceId: string } = { remoteJid: received.key.remoteJid, pushName: received.key.fromMe ? '' : received.key.fromMe == null ? '' : received.pushName, profilePicUrl: (await this.profilePicture(received.key.remoteJid)).profilePicUrl, instanceId: this.instanceId, };
          if (contactRaw.remoteJid === 'status@broadcast') {
            continue;
          }
          if (contact) {
            this.sendDataWebhook(Events.CONTACTS_UPDATE, contactRaw);
            if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
              await this.chatwootService.eventWhatsapp(
                  Events.CONTACTS_UPDATE,
                  { instanceName: this.instance.name, instanceId: this.instance.id },
                  contactRaw,
              );
            }
            if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
              await this.prismaRepository.contact.upsert({ where: { remoteJid_instanceId: { remoteJid: contactRaw.remoteJid, instanceId: contactRaw.instanceId } }, create: contactRaw, update: contactRaw, });
            continue;
          }
          this.sendDataWebhook(Events.CONTACTS_UPSERT, contactRaw);
          if (this.configService.get<Database>('DATABASE').SAVE_DATA.CONTACTS)
            await this.prismaRepository.contact.upsert({ where: { remoteJid_instanceId: { remoteJid: contactRaw.remoteJid, instanceId: contactRaw.instanceId } }, update: contactRaw, create: contactRaw, });
          if (contactRaw.remoteJid.includes('@s.whatsapp')) {
            await saveOnWhatsappCache([{ remoteJid: contactRaw.remoteJid }]);
          }
        }
      } catch (error) {
        this.logger.error(error);
      }
    },
    'messages.update': async (args: WAMessageUpdate[], settings: any) => {
      this.logger.info(`Received messages.update event with ${args.length} updates.`);
      for (const update of args) {
        if (update.update.messageStubType === proto.Message.MessageStubType.REVOKE) {
          await this.prismaRepository.messageUpdate.create({
            data: {
              fromMe: update.key.fromMe,
              keyId: update.key.id,
              remoteJid: update.key.remoteJid,
              status: 'REVOKE',
              instanceId: this.instanceId,
            },
          });
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
            this.chatwootService.eventWhatsapp(
                Events.MESSAGES_REVOKE,
                { instanceName: this.instance.name, instanceId: this.instance.id },
                update,
            );
          this.sendDataWebhook(Events.MESSAGES_REVOKE, update);
          continue;
        }

        if (update.update.messageStubType === proto.Message.MessageStubType.NONE) {
          const messageStatus = status[update.update.status as number];

          await this.prismaRepository.messageUpdate.create({
            data: {
              fromMe: update.key.fromMe,
              keyId: update.key.id,
              remoteJid: update.key.remoteJid,
              status: messageStatus,
              instanceId: this.instanceId,
            },
          });

          const msg = await this.prismaRepository.message.findFirst({
            where: { instanceId: this.instanceId, key: { path: ['id'], equals: update.key.id } },
          });

          if (msg && msg.status !== messageStatus) {
            await this.prismaRepository.message.update({ where: { id: msg.id }, data: { status: messageStatus } });
          }

          if (
              this.configService.get<Chatwoot>('CHATWOOT').ENABLED &&
              this.localChatwoot?.enabled &&
              !update.key.id.includes('@broadcast')
          )
            this.chatwootService.eventWhatsapp(
                Events.MESSAGES_UPDATE,
                { instanceName: this.instance.name, instanceId: this.instance.id },
                update,
            );

          this.sendDataWebhook(Events.MESSAGES_UPDATE, update);

          continue;
        }

        if (update.update.pollUpdates) {
          if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
            this.chatwootService.eventWhatsapp(
                Events.MESSAGES_POLL_VOTE,
                { instanceName: this.instance.name, instanceId: this.instance.id },
                update,
            );
          }

          this.sendDataWebhook(Events.MESSAGES_POLL_VOTE, update);

          const { votes, message } = await getAggregateVotesInPollMessage(
              { key: update.key, message: await this.getMessage(update.key) },
              update.update.pollUpdates,
          );

          await this.prismaRepository.message.update({
            where: { id: (await this.getMessage(update.key, true))?.['id'] },
            data: { message: message as any },
          });
        }
      }
    },

    'message-receipt.update': async (updates: MessageUserReceiptUpdate[]) => {
      this.logger.info(`Received message-receipt.update event with ${updates.length} updates.`);
      for (const { key, receipt } of updates) {
        const { remoteJid, id } = key;

        if (receipt.readByMe) {
          this.logger.log(`Update read messages ${remoteJid}`);
          await this.updateMessagesReadedByTimestamp(remoteJid, receipt.timestamp);
        } else {
          this.logger.log(`Update not read messages ${remoteJid}`);
          await this.updateChatUnreadMessages(remoteJid);
        }

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
          this.chatwootService.eventWhatsapp(
              Events.MESSAGES_RECEIPT_UPDATE,
              { instanceName: this.instance.name, instanceId: this.instance.id },
              updates,
          );

        this.sendDataWebhook(Events.MESSAGES_RECEIPT_UPDATE, updates);
      }
    },

    'groups.upsert': async (groups: GroupMetadata[]) => {
      this.logger.info(`Received groups.upsert event with ${groups.length} groups.`);
      await Promise.all(
          groups.map(async (group) => {
            await this.prismaRepository.group.upsert({
              where: { gJid_instanceId: { gJid: group.id, instanceId: this.instanceId } },
              create: {
                gJid: group.id,
                name: group.subject,
                desc: group.desc,
                instanceId: this.instanceId,
              },
              update: { name: group.subject, desc: group.desc },
            });
          }),
      );
      this.sendDataWebhook(Events.GROUPS_UPSERT, groups);
    },

    'groups.update': async (updates: Partial<GroupMetadata>[]) => {
      this.logger.info(`Received groups.update event with ${updates.length} updates.`);
      for (const update of updates) {
        await this.prismaRepository.group.updateMany({
          where: { instanceId: this.instanceId, gJid: update.id },
          data: { name: update.subject, desc: update.desc, owner: update.owner },
        });

        if (update.participants) {
          await this.prismaRepository.group.update({
            where: { gJid_instanceId: { gJid: update.id, instanceId: this.instanceId } },
            data: { participants: update.participants as any },
          });
        }
      }

      this.sendDataWebhook(Events.GROUPS_UPDATE, updates);
    },

    'group-participants.update': async ({ id, participants, action }) => {
      this.logger.info(`Received group-participants.update event for group ${id}, action: ${action}, participants: ${participants.length}.`);
      const group = await this.prismaRepository.group.findUnique({
        where: { gJid_instanceId: { gJid: id, instanceId: this.instanceId } },
      });
      if (group) {
        let currentParticipants: { id: string; admin: string }[] = (group.participants as any) || [];

        for (const participantId of participants) {
          if (action === ParticipantAction.Add) {
            if (!currentParticipants.some((p) => p.id === participantId)) {
              currentParticipants.push({ id: participantId, admin: null });
            }
          } else if (action === ParticipantAction.Remove) {
            currentParticipants = currentParticipants.filter((p) => p.id !== participantId);
          } else if (action === ParticipantAction.Promote) {
            const index = currentParticipants.findIndex((p) => p.id === participantId);
            if (index !== -1) {
              currentParticipants[index].admin = 'admin';
            }
          } else if (action === ParticipantAction.Demote) {
            const index = currentParticipants.findIndex((p) => p.id === participantId);
            if (index !== -1) {
              currentParticipants[index].admin = null;
            }
          }
        }

        await this.prismaRepository.group.update({
          where: { gJid_instanceId: { gJid: id, instanceId: this.instanceId } },
          data: { participants: currentParticipants as any },
        });
      }

      this.sendDataWebhook(Events.GROUPS_PARTICIPANTS_UPDATE, { id, participants, action });
    },

    'presence.update': async ({ id, presences }) => {
      this.logger.info(`Received presence.update event for id ${id}, presence type: ${Object.keys(presences)[0]}.`);
      this.sendDataWebhook(Events.PRESENCE_UPDATE, { id, presences });
    },

    'message-receipt.update': async (updates: MessageUserReceiptUpdate[]) => {
      this.logger.info(`Received message-receipt.update event with ${updates.length} updates.`);
      for (const { key, receipt } of updates) {
        const { remoteJid, id } = key;

        if (receipt.readByMe) {
          this.logger.log(`Update read messages ${remoteJid}`);
          await this.updateMessagesReadedByTimestamp(remoteJid, receipt.timestamp);
        } else {
          this.logger.log(`Update not read messages ${remoteJid}`);
          await this.updateChatUnreadMessages(remoteJid);
        }

        if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled)
          this.chatwootService.eventWhatsapp(
              Events.MESSAGES_RECEIPT_UPDATE,
              { instanceName: this.instance.name, instanceId: this.instance.id },
              updates,
          );

        this.sendDataWebhook(Events.MESSAGES_RECEIPT_UPDATE, updates);
      }
    },
    'messages.reaction': async (reactions: proto.IReaction[]) => {
      this.logger.info(`Received messages.reaction event with ${reactions.length} reactions.`);
      if (this.configService.get<Chatwoot>('CHATWOOT').ENABLED && this.localChatwoot?.enabled) {
        this.chatwootService.eventWhatsapp(
            Events.MESSAGES_REACTION,
            { instanceName: this.instance.name, instanceId: this.instance.id },
            reactions,
        );
      }
      this.sendDataWebhook(Events.MESSAGES_REACTION, reactions);
    },
    'labels.upsert': async (labels: Label[]) => {
      this.logger.info(`Received labels.upsert event with ${labels.length} labels.`);
      labels.forEach(async (label) => {
        await this.prismaRepository.label.upsert({
          where: { labelId_instanceId: { labelId: label.id, instanceId: this.instanceId } },
          create: { ...label, labelId: label.id, instanceId: this.instanceId },
          update: { ...label, labelId: label.id, instanceId: this.instanceId },
        });
      });
      this.sendDataWebhook(Events.LABELS_UPSERT, labels);
    },
    'labels.update': async (labels: Partial<Label>[]) => {
      this.logger.info(`Received labels.update event with ${labels.length} labels.`);
      labels.forEach(async (label) => {
        await this.prismaRepository.label.update({
          where: { labelId_instanceId: { labelId: label.id, instanceId: this.instanceId } },
          data: { ...label, labelId: label.id, instanceId: this.instanceId },
        });
      });
      this.sendDataWebhook(Events.LABELS_UPDATE, labels);
    },
    'labels.delete': async (labels: string[]) => {
      this.logger.info(`Received labels.delete event with ${labels.length} labels.`);
      labels.forEach(async (labelId) => {
        await this.prismaRepository.label.delete({
          where: { labelId_instanceId: { labelId, instanceId: this.instanceId } },
        });
      });
      this.sendDataWebhook(Events.LABELS_DELETE, labels);
    },
    'call': async (calls: proto.ICallNotification[]) => {
      this.logger.info(`Received call event with ${calls.length} calls.`);
      this.sendDataWebhook(Events.CALL, calls);
    },
    'chats.upsert': async (chats: Chat[]) => {
      this.logger.info(`Received chats.upsert (secondary) event with ${chats.length} chats.`);
      const existingChatIds = await this.prismaRepository.chat.findMany({
        where: { instanceId: this.instanceId },
        select: { remoteJid: true },
      });

      const existingChatIdSet = new Set(existingChatIds.map((chat) => chat.remoteJid));

      const chatsToInsert = chats
          .filter((chat) => !existingChatIdSet?.has(chat.id))
          .map((chat) => ({
            remoteJid: chat.id,
            instanceId: this.instanceId,
            name: chat.name,
            unreadMessages: chat.unreadCount !== undefined ? chat.unreadCount : 0,
          }));

      this.sendDataWebhook(Events.CHATS_UPSERT, chatsToInsert);

      if (chatsToInsert.length > 0) {
        if (this.configService.get<Database>('DATABASE').SAVE_DATA.CHATS)
          await this.prismaRepository.chat.createMany({ data: chatsToInsert, skipDuplicates: true });
      }
    },
  };

  private eventHandler() {
    this.client.ev.on('connection.update', this.connectionUpdate.bind(this));
    this.client.ev.on('messages.upsert', this.messageHandle['messages.upsert'].bind(this));
    this.client.ev.on('messages.update', this.messageHandle['messages.update'].bind(this));
    this.client.ev.on('message-receipt.update', this.messageHandle['message-receipt.update'].bind(this));
    this.client.ev.on('messaging-history.set', this.messageHandle['messaging-history.set'].bind(this));
    this.client.ev.on('chats.upsert', this.chatHandle['chats.upsert'].bind(this));
    this.client.ev.on('chats.update', this.chatHandle['chats.update'].bind(this));
    this.client.ev.on('chats.delete', this.chatHandle['chats.delete'].bind(this));
    this.client.ev.on('contacts.upsert', this.contactHandle['contacts.upsert'].bind(this));
    this.client.ev.on('contacts.update', this.contactHandle['contacts.update'].bind(this));
    this.client.ev.on('groups.upsert', this.messageHandle['groups.upsert'].bind(this));
    this.client.ev.on('groups.update', this.messageHandle['groups.update'].bind(this));
    this.client.ev.on('group-participants.update', this.messageHandle['group-participants.update'].bind(this));
    this.client.ev.on('presence.update', this.messageHandle['presence.update'].bind(this));
    this.client.ev.on('messages.reaction', this.messageHandle['messages.reaction'].bind(this));
    this.client.ev.on('labels.upsert', this.messageHandle['labels.upsert'].bind(this));
    this.client.ev.on('labels.update', this.messageHandle['labels.update'].bind(this));
    this.client.ev.on('labels.delete', this.messageHandle['labels.delete'].bind(this));
    this.client.ev.on('call', this.messageHandle['call'].bind(this));
  }

  private prepareMessage(
      message: proto.IWebMessageInfo,
  ): Omit<Message, 'createdAt' | 'updatedAt' | 'deletedAt'> {
    let messageType: MessageSubtype = 'UNKNOWN';

    if (message?.message) {
      messageType = getContentType(message.message) as MessageSubtype;
    }

    let mediaType: TypeMediaMessage | null = null;
    let url: string | null = null;
    let caption: string | null = null;

    if (messageType === 'imageMessage') {
      mediaType = 'image';
      url = message?.message?.imageMessage?.url || null;
      caption = message?.message?.imageMessage?.caption || null;
    } else if (messageType === 'videoMessage') {
      mediaType = 'video';
      url = message?.message?.videoMessage?.url || null;
      caption = message?.message?.videoMessage?.caption || null;
    } else if (messageType === 'audioMessage') {
      mediaType = 'audio';
      url = message?.message?.audioMessage?.url || null;
    } else if (messageType === 'documentMessage') {
      mediaType = 'document';
      url = message?.message?.documentMessage?.url || null;
      caption = message?.message?.documentMessage?.caption || null;
    } else if (messageType === 'stickerMessage') {
      mediaType = 'sticker';
      url = message?.message?.stickerMessage?.url || null;
    }

    return {
      id: cuid(),
      instanceId: this.instanceId,
      key: message.key as any,
      pushName: message.pushName,
      messageType: messageType,
      message: message.message as any,
      messageTimestamp: message.messageTimestamp as number,
      owner: message.key.fromMe ? 'me' : 'other',
      source: 'whatsapp',
      chatwootMessageId: null,
      chatwootInboxId: null,
      chatwootConversationId: null,
      contextInfo: message.message?.extendedTextMessage?.contextInfo as any,
      status: message.key.fromMe ? status[1] : status[3],
    };
  }

  private async profilePicture(jid: string) {
    try {
      const profilePictureUrl = await this.client.profilePictureUrl(jid);

      return { profilePictureUrl };
    } catch (error) {
      return { profilePictureUrl: null };
    }
  }

  private async updateChatUnreadMessages(remoteJid: string) {
    await this.prismaRepository.chat.update({
      where: { remoteJid_instanceId: { remoteJid, instanceId: this.instanceId } },
      data: { unreadMessages: { increment: 1 } },
    });
  }

  private async updateMessagesReadedByTimestamp(remoteJid: string, timestamp: number) {
    await this.prismaRepository.chat.updateMany({
      where: { remoteJid: remoteJid, instanceId: this.instanceId, unreadMessages: { gt: 0 } },
      data: { unreadMessages: 0 },
    });

    await this.prismaRepository.message.updateMany({
      where: {
        instanceId: this.instanceId,
        messageTimestamp: { lte: timestamp },
        key: { path: ['remoteJid'], equals: remoteJid },
        NOT: { status: 'READ' },
      },
      data: { status: 'READ' },
    });

    await this.prismaRepository.messageUpdate.create({
      data: {
        fromMe: false,
        keyId: null,
        remoteJid: remoteJid,
        status: 'READ',
        instanceId: this.instanceId,
        messageId: null,
      },
    });
  }

  private async historySyncNotification(msg: proto.Message.IHistorySyncNotification) {
    if (this.localSettings.syncFullHistory) {
      this.logger.info(`Sync full history message: ${JSON.stringify(msg)}`);
      await this.messageHandle['messaging-history.set'](msg);
      return true;
    }
    return false;
  }

  private async getGroupMetadataCache(jid: string) {
    let group = groupMetadataCache.get(jid);
    if (!group) {
      group = await this.client.groupMetadata(jid);
      groupMetadataCache.set(jid, group);
    }
    return group;
  }
}
