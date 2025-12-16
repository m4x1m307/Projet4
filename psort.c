/***********************************************
 * Projet 4 : Tri parallèle
 * 
 * Numéro de groupe : 3
 * Étudiants     : Maxime Lawson , Thomas Colangelo
 * 
 * Si vous utilisez plusieurs fichiers, expliquez ici comment compiler votre
 * programme :
 * 
 * 
 *
 * Expliquez ici le fonctionnement de votre algorithme de tri,
 * et comment il est parallélisé (mentionnez vos sources si pertinent) :
 * 
 * 
 * 
 * 
 * 
 *
 ***********************************************/

 // TODO : Votre code ici
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <string.h>

pthread_mutex_t mutex;
pthread_cond_t cond;

// Les trois fonction qui suivent permettent d'implémenter le tri par fusion
void split(size_t* t, int n, size_t** t1, int* n1, size_t** t2, int* n2){
    *n1 = n / 2;
    *n2 = n - *n1;
    *t1 = malloc(*n1 * sizeof(size_t));
    *t2 = malloc(*n2 * sizeof(size_t));

    memcpy(*t1, t, *n1 * sizeof(size_t));
    memcpy(*t2, t + (*n1), *n2 * sizeof(size_t));
}

size_t* merge(size_t* t1, int n1, size_t* t2, int n2, char* mmap_adr){
    size_t* out = malloc((n1+n2) * sizeof(size_t));
    int i = 0, j = 0, k = 0;

    while(i < n1 && j < n2){
        int cmp = memcmp(mmap_adr + t1[i],mmap_adr + t2[j], 4);

        if(cmp < 0){
            out[k++] = t1[i++];
        }else{
            out[k++] = t2[j++];
        }
    }

    while(i < n1){
        out[k++] = t1[i++];
    }
    while(j < n2){
        out[k++] = t2[j++];
    }

    return out;
}

size_t* merge_sort(size_t* t, int n, char* mmap_adr){
    if(n <= 1)
        return t;
    
    size_t *t1, *t2;
    int n1, n2;

    split(t, n, &t1, &n1, &t2, &n2);

    t1 = merge_sort(t1, n1, mmap_adr);
    t2 = merge_sort(t2, n2, mmap_adr);

    size_t* res = merge(t1, n1, t2, n2, mmap_adr);

    free(t1);
    free(t2);

    return res;
}

/*
void* sort(void* arg){
    char* tab = (char*) arg;

}
*/

int main(int argc, char* argv[]){
    /*if(argc < 4)
        fprintf(stderr, "Error: not enough parameters were given");
    
    pthread_mutex_init(&mutex, NULL); //initialise le verrou
    pthread_cond_init(&cond, NULL); //initialise la variable de condition
    int len = atoi(argv[3]); //nombre maximal de thread a utiliser (paramètre nb_threads)
    pthread_t* threads = malloc(len * sizeof(pthread_t)); //créer le tableau de thread
    assert(threads != NULL);*/

    int fd = open(argv[1], O_RDONLY);
    assert(fd != -1);

    struct stat st;
    assert(fstat(fd, &st) != -1);

    char* mmap_adr = (char*) mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    assert(mmap_adr != MAP_FAILED);

    size_t size = st.st_size; //taille du fichier

    size_t key_num = size / 100;

    size_t* offsets = malloc(key_num * sizeof(size_t));
    assert(offsets != NULL);

    for(size_t i=0; i < key_num; i++)
        offsets[i] = i * 100;

    size_t* sorted = merge_sort(offsets, key_num, mmap_adr);

    FILE* output = fopen(argv[2], "wb");
    assert(output != NULL);

    for(size_t i=0; i < key_num; i++){ // affichage de test
        //write(STDOUT_FILENO, mmap_adr + sorted[i], 100);
        fwrite(mmap_adr + sorted[i], 1, 100, output);
    }

    if(fsync(fd) != 0)
        fprintf(stderr, "Error : fsync got a problem");

    free(offsets);
    munmap(mmap_adr, st.st_size);
    close(fd);

    return(EXIT_SUCCESS);
}