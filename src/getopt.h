#ifdef _WIN32

extern int opterr,         /* if error message should be printed */
  optind,                  /* index into parent argv vector */
  optopt,                  /* character checked for validity */
  optreset;                /* reset getopt */
extern const char *optarg; /* argument associated with option */

int getopt(int nargc, char * const nargv[], const char *ostr);
#else
#include <unistd.h> // IWYU pragma: export
#endif
