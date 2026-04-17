// object.c — Content-addressable object store
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>
#include <limits.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTATION ──────────────────────────────────────────────────────────

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    // 1. Build header
    char header[128];
    const char *type_str = (type == OBJ_BLOB) ? "blob" : (type == OBJ_TREE) ? "tree" : "commit";
    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len);
    header[header_len++] = '\0'; // Include the null terminator in the object

    // 2. Combine header and data
    size_t total_size = header_len + len;
    unsigned char *full_content = malloc(total_size);
    if (!full_content) return -1;
    memcpy(full_content, header, header_len);
    memcpy(full_content + header_len, data, len);

    // 3. Compute hash and check deduplication
    compute_hash(full_content, total_size, id_out);
    if (object_exists(id_out)) {
        free(full_content);
        return 0;
    }

    // 4. Create shard directory
    char path[PATH_MAX], dir[PATH_MAX];
    object_path(id_out, path, sizeof(path));
    
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id_out, hex);
    snprintf(dir, sizeof(dir), "%s/%.2s", OBJECTS_DIR, hex);
    
    mkdir(OBJECTS_DIR, 0755);
    mkdir(dir, 0755);

    // 5. Write to temporary file (atomic write pattern)
    char temp_path[PATH_MAX + 8];
    snprintf(temp_path, sizeof(temp_path), "%s.tmp", path);
    
    int fd = open(temp_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fd < 0) { free(full_content); return -1; }
    
    if (write(fd, full_content, total_size) != (ssize_t)total_size) {
        close(fd); unlink(temp_path); free(full_content); return -1;
    }
    
    fsync(fd);
    close(fd);

    // 6. Rename temp file to final path
    if (rename(temp_path, path) < 0) {
        unlink(temp_path); free(full_content); return -1;
    }

    free(full_content);
    return 0;
}

int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[PATH_MAX];
    object_path(id, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    // Get file size
    fseek(f, 0, SEEK_END);
    long total_size = ftell(f);
    rewind(f);

    unsigned char *buf = malloc(total_size);
    if (!buf) { fclose(f); return -1; }
    if (fread(buf, 1, total_size, f) != (size_t)total_size) {
        fclose(f); free(buf); return -1;
    }
    fclose(f);

    // Verify Integrity
    ObjectID actual_id;
    compute_hash(buf, total_size, &actual_id);
    if (memcmp(id->hash, actual_id.hash, HASH_SIZE) != 0) {
        free(buf); return -1;
    }

    // Parse Type from Header
    if (strncmp((char*)buf, "blob", 4) == 0) *type_out = OBJ_BLOB;
    else if (strncmp((char*)buf, "tree", 4) == 0) *type_out = OBJ_TREE;
    else if (strncmp((char*)buf, "commit", 4) == 0) *type_out = OBJ_COMMIT;

    // Locate Data start (after the \0)
    char *null_terminator = memchr(buf, '\0', total_size);
    if (!null_terminator) { free(buf); return -1; }
    
    size_t header_len = (null_terminator - (char*)buf) + 1;
    *len_out = total_size - header_len;
    
    *data_out = malloc(*len_out);
    if (!*data_out) { free(buf); return -1; }
    memcpy(*data_out, null_terminator + 1, *len_out);

    free(buf);
    return 0;
}
