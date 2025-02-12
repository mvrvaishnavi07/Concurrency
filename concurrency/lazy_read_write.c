#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <errno.h> 
#define MAX_REQUESTS 1000
#define YELLOW "\033[33m"
#define PINK "\033[95m"
#define GREEN "\033[32m"
#define RED "\033[31m"
#define WHITE "\033[37m"
#define RESET "\033[0m"

// Operation types
typedef enum {
    READ,
    WRITE,
    DELETE
} Operation;

// Request structure
typedef struct {
    int user_id;
    int file_id;
    Operation operation;
    int request_time;
    int timeout_time;
} Request;

// File structure
typedef struct {
    int readers;
    int writers;
    int is_deleted;
    int deletion_in_progress;
    int concurrent_users;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} File;

File *files;
// Global variables
// File files[MAX_FILES];
Request requests[MAX_REQUESTS];
int num_requests = 0;
int read_time, write_time, delete_time;
int num_files, concurrent_limit, timeout;
pthread_mutex_t print_lock = PTHREAD_MUTEX_INITIALIZER;
time_t start_time;


// int compare_requests(const void *a, const void *b) {
//     Request *reqA = (Request *)a;
//     Request *reqB = (Request *)b;
//     return reqA->request_time - reqB->request_time;
// }

// Comparison function for sorting requests based on arrival time and priority
int compare_requests(const void *a, const void *b) {
    Request *reqA = (Request *)a;
    Request *reqB = (Request *)b;

    // Primary sorting by arrival time
    if (reqA->request_time != reqB->request_time) {
        return reqA->request_time - reqB->request_time;
    }

    // Secondary sorting by priority: READ > WRITE > DELETE
    if (reqA->operation == reqB->operation) {
        return 0; // Same operation, no priority difference
    } else if (reqA->operation == READ) {
        return -1; // READ has highest priority
    } else if (reqB->operation == READ) {
        return 1;  // READ over WRITE and DELETE
    } else if (reqA->operation == WRITE) {
        return -1; // WRITE has higher priority than DELETE
    } else {
        return 1;  // DELETE has lowest priority
    }
}


void print_message(const char* message, const char* color) {
    pthread_mutex_lock(&print_lock);
    time_t current = time(NULL);
    int elapsed = (int)(current - start_time);
    printf("%s%s at %d seconds%s\n", color, message, elapsed, RESET);
    fflush(stdout);  // Ensure immediate output
    pthread_mutex_unlock(&print_lock);
}

// Initialize file
void init_file(File* file) {
    file->readers = 0;
    file->writers = 0;
    file->is_deleted = 0;
    file->deletion_in_progress = 0;
    file->concurrent_users = 0;
    pthread_mutex_init(&file->lock, NULL);
    pthread_cond_init(&file->cond, NULL);
}

const char* operation_to_string(Operation op) {
    switch(op) {
        case READ: return "READ";
        case WRITE: return "WRITE";
        case DELETE: return "DELETE";
        default: return "UNKNOWN";
    }
}

// Process a request

int should_cancel_request(Request* req, time_t current) {
    int elapsed_time = (int)(current - start_time);
    return elapsed_time >= (req->request_time + timeout);
}

void* process_request(void* arg) {
    Request* req = (Request*)arg;
    File* file = &files[req->file_id - 1];
    char message[256];
    
    // Wait until request time
    time_t current = time(NULL);
    int elapsed = (int)(current - start_time);
    if (req->request_time > elapsed) {
        sleep(req->request_time - elapsed);
    }
    
    // Print request received
    snprintf(message, sizeof(message), 
            "User %d has made request for performing %s on file %d", 
            req->user_id, 
            operation_to_string(req->operation),
            req->file_id);
    print_message(message, YELLOW);

    // Wait 1 second before processing
    sleep(1);

    pthread_mutex_lock(&file->lock);

    // Check if file is deleted
    if (file->is_deleted || (file->deletion_in_progress)) {
        pthread_mutex_unlock(&file->lock);
        snprintf(message, sizeof(message),
                "LAZY has declined the request of User %d because an invalid/deleted file was requested", 
                req->user_id);  
        print_message(message, WHITE);
        return NULL;
    }
    current = time(NULL);
    if (should_cancel_request(req, current)) {
        // printf("here1\n");
        pthread_mutex_unlock(&file->lock);
        snprintf(message, sizeof(message),
            "User %d canceled the request due to no response", 
            req->user_id);
        print_message(message, RED);
        return NULL;
    }
    // For DELETE operations, wait for no readers and writers
    if (req->operation == DELETE) {
        // file->deletion_in_progress = 1;
        while (file->readers > 0 || file->writers > 0 || file->deletion_in_progress) {
            // Create a timeout checker thread
            pthread_t timeout_thread;
            int thread_created = 0;
            
            // Check if we should timeout before waiting
            current = time(NULL);
            if (should_cancel_request(req, current)) {
                // printf("here1\n");
                pthread_mutex_unlock(&file->lock);
                snprintf(message, sizeof(message),
                        "User %d canceled the request due to no response", 
                        req->user_id);
                print_message(message, RED);
                return NULL;
            }
            if (file->is_deleted || (file->deletion_in_progress)) {
                pthread_mutex_unlock(&file->lock);
                snprintf(message, sizeof(message),
                        "LAZY has declined the request of User %d because an invalid/deleted file was requested", 
                        req->user_id);  
                print_message(message, WHITE);
                return NULL;
            }
            
            // Wait with timeout
            struct timespec wait_time;
            wait_time.tv_sec = start_time + req->timeout_time;
            wait_time.tv_nsec = 0;
            
            int wait_result = pthread_cond_timedwait(&file->cond, &file->lock, &wait_time);
            
            if (should_cancel_request(req, time(NULL))) {
                // printf("here2\n");
                pthread_mutex_unlock(&file->lock);
                snprintf(message, sizeof(message),
                        "User %d canceled the request due to no response", 
                        req->user_id);
                print_message(message, RED);
                return NULL;
            }
        }

        file->deletion_in_progress = 1; 
        
    }
    // For WRITE operations, wait if another write is in progress
    else if (req->operation == WRITE ) {
        while (file->writers > 0 || file->deletion_in_progress) {
            // Check timeout before waiting
            current = time(NULL);
            if (should_cancel_request(req, current)) {
                pthread_mutex_unlock(&file->lock);
                snprintf(message, sizeof(message),
                        "User %d canceled the request due to no response", 
                        req->user_id);
                print_message(message, RED);
                return NULL;
            }
            if (file->is_deleted || (file->deletion_in_progress)) {
                pthread_mutex_unlock(&file->lock);
                snprintf(message, sizeof(message),
                        "LAZY has declined the request of User %d because an invalid/deleted file was requested", 
                        req->user_id);  
                print_message(message, WHITE);
                return NULL;
            }
            
            struct timespec wait_time;
            wait_time.tv_sec = start_time + req->timeout_time;
            wait_time.tv_nsec = 0;
            
            int wait_result = pthread_cond_timedwait(&file->cond, &file->lock, &wait_time);
            
            if (should_cancel_request(req, time(NULL))) {
                pthread_mutex_unlock(&file->lock);
                snprintf(message, sizeof(message),
                        "User %d canceled the request due to no response", 
                        req->user_id);
                print_message(message, RED);
                return NULL;
            }
        }
    }

    // Check concurrent users limit
    while (file->concurrent_users >= concurrent_limit) {
        // Check timeout before waiting
        current = time(NULL);
        if (should_cancel_request(req, current)) {
            pthread_mutex_unlock(&file->lock);
            snprintf(message, sizeof(message),
                    "User %d canceled the request due to no response", 
                    req->user_id);
            print_message(message, RED);
            return NULL;
        }
        
        struct timespec wait_time;
        wait_time.tv_sec = start_time + req->timeout_time;
        wait_time.tv_nsec = 0;
        
        int wait_result = pthread_cond_timedwait(&file->cond, &file->lock, &wait_time);
        
        if (should_cancel_request(req, time(NULL))) {
            pthread_mutex_unlock(&file->lock);
            snprintf(message, sizeof(message),
                    "User %d canceled the request due to no response", 
                    req->user_id);
            print_message(message, RED);
            return NULL;
        }
    }

    // Update file state based on operation
    switch (req->operation) {
        case READ:
            file->readers++;
            break;
        case WRITE:
            file->writers++;
            break;
    }
    file->concurrent_users++;
    pthread_mutex_unlock(&file->lock);

    // Print processing started
    snprintf(message, sizeof(message),
            "LAZY has taken up the request of User %d", 
            req->user_id);
    print_message(message, PINK);

    // Simulate processing time
    int sleeptime ;
    if(req->operation == READ){
        sleeptime = read_time;
    }
    else if(req->operation == WRITE){
        sleeptime = write_time;
    }
    else if(req->operation == DELETE){
        sleeptime = delete_time;
    }
    sleep(sleeptime);

    pthread_mutex_lock(&file->lock);
    
    // Cleanup after operation
    switch (req->operation) {
        case READ:
            file->readers--;
            break;
        case WRITE:
            file->writers--;
            break;
        case DELETE:
            file->is_deleted = 1;
            break;
    }
    file->concurrent_users--;
    
    // Signal waiting threads
    pthread_cond_broadcast(&file->cond);
    pthread_mutex_unlock(&file->lock);

    // Print completion
    snprintf(message, sizeof(message),
            "The request for User %d was completed", 
            req->user_id);
    print_message(message, GREEN);

    return NULL;
}
int main() {
    // Read configuration
    scanf("%d %d %d", &read_time, &write_time, &delete_time);
    scanf("%d %d %d", &num_files, &concurrent_limit, &timeout);

    files = malloc(sizeof(File) * num_files);
    if (files == NULL) {
        printf("Memory allocation failed for files array.\n");
        exit(EXIT_FAILURE);
    }
    // Initialize files
    for (int i = 0; i < num_files; i++) {
        init_file(&files[i]);
    }

    // Read requests
    char operation_str[10];
    while (1) {
        if (scanf("%d", &requests[num_requests].user_id) != 1) {
            char stop[10];
            scanf("%s", stop);
            if (strcmp(stop, "STOP") == 0) break;
        }
        scanf("%d %s %d", 
              &requests[num_requests].file_id,
              operation_str,
              &requests[num_requests].request_time);
        

        if (requests[num_requests].file_id < 1 || requests[num_requests].file_id > num_files) {
            printf(RED "Error: Invalid file access by User %d for File ID %d. Declining the request.\n" RESET,
                requests[num_requests].user_id, requests[num_requests].file_id);
            continue;  
        }

        if (requests[num_requests].request_time < 0){
            printf(RED "Error: Invalid request time by User %d . Declining the request.\n" RESET,
                requests[num_requests].user_id);
            continue; 
        }
    

        if (strcmp(operation_str, "READ") == 0) {
            requests[num_requests].operation = READ;
        } else if (strcmp(operation_str, "WRITE") == 0) {
            requests[num_requests].operation = WRITE;
        } else if (strcmp(operation_str, "DELETE") == 0) {
            requests[num_requests].operation = DELETE;
        } else {
            printf(RED "Error: Invalid operation '%s' by User %d. Declining the request.\n" RESET, 
                operation_str, requests[num_requests].user_id);
            continue;  // Skip this invalid request
        }

        requests[num_requests].timeout_time = requests[num_requests].request_time + timeout;
        num_requests++;
    }

    // Sort requests by arrival time
    qsort(requests, num_requests, sizeof(Request), compare_requests);

    // printf("Requests after sorting:\n");
    // for (int i = 0; i < num_requests; i++) {
    //     printf("User %d requested %s on file %d at %d seconds\n",
    //         requests[i].user_id,
    //         operation_to_string(requests[i].operation),
    //         requests[i].file_id,
    //         requests[i].request_time);
    // }

    printf("\nLAZY has woken up!\n");
    
    // Record start time
    start_time = time(NULL);

    // Create threads for each request
    pthread_t threads[MAX_REQUESTS];
    for (int i = 0; i < num_requests; i++) {
        usleep(5000);
        pthread_create(&threads[i], NULL, process_request, &requests[i]);
    }

    // Wait for all threads to complete
    for (int i = 0; i < num_requests; i++) {
        pthread_join(threads[i], NULL);
    }

    printf("\nLAZY has no more pending requests and is going back to sleep!\n");

    // Cleanup
    for (int i = 0; i < num_files; i++) {
        pthread_mutex_destroy(&files[i].lock);
        pthread_cond_destroy(&files[i].cond);
    }
    free(files);
    pthread_mutex_destroy(&print_lock);

    return 0;
}








// Request* get_next_priority_request(time_t current_time) {
//     pthread_mutex_lock(&request_lock);
//     Request* best_request = NULL;
//     int elapsed = (int)(current_time - start_time);
    
//     // First pass: Look for READ operations
//     for (int i = 0; i < num_requests; i++) {
//         if (!requests[i].is_ready && 
//             requests[i].request_time <= elapsed && 
//             requests[i].operation == READ &&
//             !should_cancel_request(&requests[i], current_time)) {
//             best_request = &requests[i];
//             break;
//         }
//     }
    
//     // Second pass: Look for WRITE operations if no READ was found
//     if (!best_request) {
//         for (int i = 0; i < num_requests; i++) {
//             if (!requests[i].is_ready && 
//                 requests[i].request_time <= elapsed && 
//                 requests[i].operation == WRITE &&
//                 !should_cancel_request(&requests[i], current_time)) {
//                 best_request = &requests[i];
//                 break;
//             }
//         }
//     }
    
//     // Last pass: Look for DELETE operations if no READ/WRITE was found
//     if (!best_request) {
//         for (int i = 0; i < num_requests; i++) {
//             if (!requests[i].is_ready && 
//                 requests[i].request_time <= elapsed && 
//                 requests[i].operation == DELETE &&
//                 !should_cancel_request(&requests[i], current_time)) {
//                 best_request = &requests[i];
//                 break;
//             }
//         }
//     }
    
//     if (best_request) {
//         best_request->is_ready = 1;
//     }
    
//     pthread_mutex_unlock(&request_lock);
//     return best_request;
// }

// pthread_t threads[MAX_REQUESTS];
//     int thread_count = 0;
//     Request* current_request;
    
//     while (thread_count < num_requests) {
//         current_request = get_next_priority_request(time(NULL));
//         if (current_request) {
//             pthread_create(&threads[thread_count], NULL, process_request, current_request);
//             thread_count++;
//             usleep(5000);  // Small delay between thread creations
//         }
//         usleep(1000);  // Small delay to check for new ready requests
//     }
// pthread_mutex_t request_lock = PTHREAD_MUTEX_INITIALIZER; 