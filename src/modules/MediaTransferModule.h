#pragma once

#include "ProtobufModule.h"
#include "concurrency/OSThread.h"
#include "mesh/generated/meshtastic/media_transfer.pb.h"

#include <unordered_map>
#include <vector>

// Callback invoked when an incoming transfer completes successfully
typedef void (*TransferCompleteCallback)(const uint8_t *data, uint32_t size,
                                          meshtastic_MediaContentType contentType,
                                          uint32_t fromNode, uint32_t transferId);

// Tracks state for an outgoing media transfer
struct OutgoingTransfer {
    uint32_t transferId;
    uint32_t destNodeId;
    uint8_t channelIndex;
    uint32_t totalChunks;
    uint32_t totalSize;
    uint32_t checksum;
    meshtastic_MediaContentType contentType;
    uint32_t nextChunkToSend;    // next chunk index to transmit
    uint32_t startTime;          // millis() when transfer started
    uint32_t lastChunkTime;      // millis() when last chunk was sent
    bool complete;               // all chunks sent at least once
    std::vector<uint8_t> data;   // full compressed payload
    std::vector<uint32_t> nackedChunks; // chunks that need retransmission
};

// Tracks state for an incoming media transfer
struct IncomingTransfer {
    uint32_t transferId;
    uint32_t fromNodeId;
    uint32_t totalChunks;
    uint32_t totalSize;
    uint32_t checksum;
    meshtastic_MediaContentType contentType;
    uint32_t startTime;          // millis() when first chunk received
    uint32_t lastChunkTime;      // millis() when last chunk received
    std::vector<bool> receivedChunks; // bitmap of received chunks
    std::vector<uint8_t> data;   // reassembly buffer
    char mimeType[32];
    uint32_t durationSeconds;
    uint32_t width;
    uint32_t height;
};

class MediaTransferModule : private concurrency::OSThread, public ProtobufModule<meshtastic_MediaTransfer>
{
  public:
    MediaTransferModule();

    // Start sending a media file to a specific node
    // data must be already compressed by the companion app
    // Returns transfer_id, or 0 on failure
    uint32_t startTransfer(uint32_t destNodeId, uint8_t channelIndex,
                           const uint8_t *compressedData, uint32_t dataSize,
                           meshtastic_MediaContentType contentType,
                           uint32_t checksum, const char *mimeType = "",
                           uint32_t durationSeconds = 0, uint32_t width = 0, uint32_t height = 0);

    // Cancel an outgoing transfer
    void cancelTransfer(uint32_t transferId);

    // Get progress of an outgoing transfer (0-100)
    int getTransferProgress(uint32_t transferId);

    // Register a callback for completed incoming transfers
    void setTransferCompleteCallback(TransferCompleteCallback cb) { completionCallback = cb; }

  protected:
    virtual int32_t runOnce() override;
    virtual bool handleReceivedProtobuf(const meshtastic_MeshPacket &mp, meshtastic_MediaTransfer *decoded) override;

  private:
    static constexpr uint8_t MAX_CONCURRENT_TRANSFERS = 4;
    static constexpr uint32_t CHUNK_SEND_INTERVAL_MS = 2000;  // time between chunks
    static constexpr uint32_t NACK_TIMEOUT_MS = 30000;        // wait for NACK before declaring complete
    static constexpr uint32_t TRANSFER_TIMEOUT_MS = 1800000;  // 30 min max transfer time

    uint32_t nextTransferId = 1;
    std::vector<OutgoingTransfer> outgoing;
    std::vector<IncomingTransfer> incoming;

    uint32_t getChunkSize();
    uint32_t generateTransferId();

    // Send handlers
    void sendStartPacket(OutgoingTransfer &xfer);
    void sendNextChunk(OutgoingTransfer &xfer);
    void sendCompletePacket(OutgoingTransfer &xfer);
    void sendCancelPacket(uint32_t transferId, uint8_t channelIndex);

    // Receive handlers
    void handleMediaStart(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded);
    void handleMediaChunk(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded);
    void handleMediaComplete(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded);
    void handleMediaNack(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded);
    void handleMediaAckComplete(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded);
    void handleMediaCancel(const meshtastic_MeshPacket &mp, const meshtastic_MediaTransfer &decoded);

    // Send NACK for missing chunks
    void sendNack(uint8_t channelIndex, uint32_t transferId, const std::vector<uint32_t> &missingChunks);
    // Send ACK_COMPLETE when all chunks received and verified
    void sendAckComplete(uint8_t channelIndex, uint32_t transferId);

    // Send a MediaTransfer protobuf on a channel to a specific node
    void sendMediaPacket(uint8_t channelIndex, uint32_t destNodeId, const meshtastic_MediaTransfer &payload);

    // CRC32 calculation
    static uint32_t crc32(const uint8_t *data, uint32_t length);

    TransferCompleteCallback completionCallback = nullptr;
};

extern MediaTransferModule *mediaTransferModule;
