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

pthread_mutex_t mutex;
pthread_cond_t cond;

// Les trois fonction qui suivent permettent d'implémenter le tri par fusion
void split(char* t, int n, char** t1, int* n1, char** t2, int* n2){
    *n1 = n / 2;
    *n2 = n - *n1;
    *t1 = mallloc(*n1 * sizeof(char));
    *t2 = mallloc(*n2 * sizeof(char));

    for(int i=0; i < *n1; i++)
        (*t1)[i] = t[i];
    for(int i=0; i < *n2; i++)
        (*t2)[i] = t[i];
}

char* merge(char* t1, int n1, char* t2, int n2){
    int* out = malloc((n1+n2) * sizeof(char));
    int i = 0, j = 0, k = 0;

    while(i < n1 && j < n2){
        if(t1[i] < t2[j])
            out[k++] = t1[i++];
        else
            out[k++] = t2[j++];
    }

    while(i < n1)
        out[k++] = t1[i++];
    while(j < n2)
        out[k++] = t2[j++];

    return out;
}

char* merge_sort(char* t, int n){
    if(n <= 1)
        return t;
    
    char *t1, *t2;
    int n1, n2;

    split(t, n, &t1, &n1, &t2, &n2);

    t1 = merge_sort(t1, n1);
    t2 = merge_sort(t2, n2);

    char* res = merge(t1, n1, t2, n2);

    free(t1);
    free(t2);

    return res;
}


void* sort(void* arg){
    char* tab = (char*) arg;

    int n = strlen(tab);
    char* res = merge_sort(tab, n);

    pthread_exit(&res);
}

int main(int argc, char* argv[]){
    if(argc < 4)
        fpintf(stderr, "Error: not enough parameters were given");
    
    pthread_mutex_init(&mutex, NULL); //initialise le verrou
    pthread_cond_init(&cond, NULL); //initialise la variable de condition
    int len = atoi(argv[3]); //nombre maximal de thread a utiliser (paramètre nb_threads)
    pthread_t* threads = malloc(len * sizeof(pthread_t)); //créer le tableau de thread
    assert(threads != NULL);

    int fd = open(argv[1], O_RDONLY);
    assert(fd != -1);

    struct stat st;
    assert(fstat(fd, &st) != -1);

    char* mmap_adr = (char*) mmap(NULL, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    assert(mmap_adr != MAP_FAILED);

    size_t size = st.st_size; //taille du fichier

    int nt = size / len; //taille des sous tableaux que chaque thread va trier
    int reste = size % len;

    char** subTab = malloc(len * sizeof(char*)); //création du tableau qui va contenir tous les sous tableaux du fichier 
    assert(subTab != NULL);

    int k = 0;
    for(int i=0; i < len; i++){
        int this_nt = nt;
        if(i < reste) // redistribution des octets restants si besoin
            this_nt++;

        subTab[i] = malloc(this_nt * sizeof(char)); //création des sous tableaux 
        assert(subTab[i] != NULL);
        for(int j=k; j < nt; j++){
            subTab[i][j] = mmap_adr[k++]; //copie des valeurs du tableau original obtenu avec mmap dans les sous tableaux 
        }
    }

    char** results = malloc(len * sizeof(char*));


    for(int i=0; i < len; i++)
        pthread_create(&threads[i], NULL, sort, &subTab[i]); //création de tous les threads
    for(int i=0; i < len; i++)
        pthread_join(threads[i], &results[i]);
    
    
    
    


    munmap(mmap_adr, st.st_size);
    close(fd);

    return(EXIT_SUCCESS);
}