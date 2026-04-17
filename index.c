#include "index.h"
#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>

int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out);

// ─── PROVIDED ────────────────────────────────────────────────────────────────

IndexEntry* index_find(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0)
            return &index->entries[i];
    }
    return NULL;
}

int index_remove(Index *index, const char *path) {
    for (int i = 0; i < index->count; i++) {
        if (strcmp(index->entries[i].path, path) == 0) {
            int remaining = index->count - i - 1;
            if (remaining > 0)
                memmove(&index->entries[i], &index->entries[i + 1],
                        remaining * sizeof(IndexEntry));
            index->count--;
            return index_save(index);
        }
    }
    return -1;
}

int index_status(const Index *index) {
    printf("Staged changes:\n");
    for (int i = 0; i < index->count; i++) {
        printf("  staged:      %s\n", index->entries[i].path);
    }
    if (index->count == 0) printf("  (nothing to show)\n");
    printf("\n");

    printf("Unstaged changes:\n  (nothing to show)\n\n");

    printf("Untracked files:\n");
    DIR *dir = opendir(".");
    if (dir) {
        struct dirent *ent;
        while ((ent = readdir(dir)) != NULL) {
            if (ent->d_name[0] == '.') continue;
            if (strcmp(ent->d_name, "pes") == 0) continue;
            if (strstr(ent->d_name, ".o") != NULL) continue;
            if (strstr(ent->d_name, "test_") != NULL) continue;

            if (!index_find((Index*)index, ent->d_name)) {
                struct stat st;
                if (stat(ent->d_name, &st) == 0 && S_ISREG(st.st_mode)) {
                    printf("  untracked:   %s\n", ent->d_name);
                }
            }
        }
        closedir(dir);
    }
    printf("\n");
    return 0;
}

// ─── THE FIXES ───────────────────────────────────────────────────────────────

int index_load(Index *index) {
    index->count = 0;
    FILE *f = fopen(".pes/index", "r");
    if (!f) return 0; 

    char hex[65];
    unsigned int m;
    long mt, s; // Use long to match %ld and prevent stack smash

    while (index->count < MAX_INDEX_ENTRIES &&
           fscanf(f, "%o %64s %ld %ld %255s\n", &m, hex, &mt, &s, 
                  index->entries[index->count].path) == 5) {
        
        index->entries[index->count].mode = m;
        index->entries[index->count].mtime_sec = (uint32_t)mt;
        index->entries[index->count].size = (uint32_t)s;
        hex_to_hash(hex, &index->entries[index->count].hash);
        index->count++;
    }
    fclose(f);
    return 0;
}

static int compare_entries(const void *a, const void *b) {
    return strcmp(((IndexEntry*)a)->path, ((IndexEntry*)b)->path);
}

int index_save(const Index *index) {
    FILE *f = fopen(".pes/index.tmp", "w");
    if (!f) return -1;

    for (int i = 0; i < index->count; i++) {
        char hex[65];
        hash_to_hex(&index->entries[i].hash, hex);
        fprintf(f, "%o %s %ld %ld %s\n", 
                index->entries[i].mode, hex, 
                (long)index->entries[i].mtime_sec, 
                (long)index->entries[i].size, 
                index->entries[i].path);
    }
    fclose(f);
    rename(".pes/index.tmp", ".pes/index");
    return 0;
}

int index_add(Index *index, const char *path) {
    struct stat st;
    if (stat(path, &st) != 0 || !S_ISREG(st.st_mode)) return -1;

    FILE *f = fopen(path, "rb");
    if (!f) return -1;

    void *buf = malloc(st.st_size);
    if (!buf) { fclose(f); return -1; }
    
    size_t read_bytes = fread(buf, 1, st.st_size, f);
    fclose(f);

    if (read_bytes != (size_t)st.st_size) { free(buf); return -1; }

    ObjectID id;
    // Calling with proper header inclusion now
    if (object_write(OBJ_BLOB, buf, st.st_size, &id) != 0) {
        free(buf);
        return -1;
    }
    free(buf);

    IndexEntry *entry = index_find(index, path);
    if (!entry) {
        if (index->count >= MAX_INDEX_ENTRIES) return -1;
        entry = &index->entries[index->count++];
        strncpy(entry->path, path, sizeof(entry->path) - 1);
    }

    entry->mode = (st.st_mode & S_IXUSR) ? 0100755 : 0100644;
    entry->hash = id;
    entry->mtime_sec = (uint32_t)st.st_mtime;
    entry->size = (uint32_t)st.st_size;

    return index_save(index);
}
