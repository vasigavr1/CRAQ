//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_MESSAGES_H
#define ODYSSEY_CR_MESSAGES_H



#define PREP_COALESCE 8
#define R_COALESCE 20

#define MAX_PREP_WRS (1 + ((2 *MACHINE_NUM * SESSIONS_PER_THREAD ) / PREP_COALESCE))
#define CR_PREP_MES_HEADER 12 // opcode(1), coalesce_num(1) l_id (8)


typedef struct cr_prepare {
  uint64_t version;
  mica_key_t key;
  uint8_t m_id;
  uint8_t unused;
  uint16_t sess_id;
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) cr_prep_t;

// prepare message
typedef struct cr_prep_message {
  uint64_t l_id;
  uint8_t opcode;
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t unused;
  cr_prep_t prepare[PREP_COALESCE];
} __attribute__((__packed__)) cr_prep_mes_t;

typedef struct cr_prep_message_ud_req {
  uint8_t grh[GRH_SIZE];
  cr_prep_mes_t prepare;
} cr_prep_mes_ud_t;


//-------------------------
// Reads and Read Replies
//-------------------------


//---READS---
#define CR_R_MES_HEADER (10) // local id + coalesce num + m_id

#define CR_RECV_R_WRS (REM_MACH_NUM * SESSIONS_PER_THREAD) // TODO divide by coalesce
#define CR_INCOMING_R (REM_MACH_NUM * SESSIONS_PER_THREAD)
#define CR_R_WRS (1 + (SESSIONS_PER_THREAD / R_COALESCE))

#define CR_R_BUF_SLOTS (CR_RECV_R_WRS)

// READ REPLIES --
#define R_REP_MES_HEADER (9) //l_id 8 , coalesce_num 1
#define R_REP_BIG_SIZE (VALUE_SIZE + 8 + 1) // version + opcode
#define R_REP_SMALL_SIZE 1
#define R_REP_MES_SIZE (R_REP_MES_HEADER + (R_COALESCE * R_REP_BIG_SIZE))
#define MAX_R_REP_MES_SIZE R_REP_MES_SIZE
#define R_REP_SEND_SIZE MIN(MAX_R_REP_MES_SIZE, MTU)

#define MAX_R_REP_COALESCE R_COALESCE
#define MAX_REPS_IN_REP MAX_R_REP_COALESCE

#define R_REP_SEND_SIDE_PADDING FIND_PADDING(R_REP_SEND_SIZE)
#define ALIGNED_R_REP_SEND_SIDE (R_REP_SEND_SIZE + R_REP_SEND_SIDE_PADDING)
#define R_REP_RECV_SIZE (GRH_SIZE + ALIGNED_R_REP_SEND_SIDE)

#define CR_RECV_R_REP_WRS (SESSIONS_PER_THREAD)
#define CR_R_REP_WRS (SESSIONS_PER_THREAD * REM_MACH_NUM)

#define R_REP_ENABLE_INLINING ((R_REP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_REP_FIFO_SIZE (CR_RECV_R_WRS * R_COALESCE)


#define VERSION_EQUAL 21
#define VERSION_DIFF 22
//
typedef struct cr_read {
  struct key key;
  uint64_t version;
  uint8_t opcode;
  uint8_t unused;
} __attribute__((__packed__)) cr_read_t;

typedef struct cr_r_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint64_t l_id ;
  cr_read_t read[R_COALESCE];
} __attribute__((__packed__)) cr_r_mes_t;


typedef struct cr_r_message_ud_req {
  uint8_t unused[GRH_SIZE];
  cr_r_mes_t r_mes;
} cr_r_mes_ud_t;


typedef struct cr_r_rep_small {
  uint8_t opcode;
}__attribute__((__packed__)) cr_r_rep_small_t;


typedef struct cr_r_rep_big {
  uint8_t opcode;
  uint64_t version;
  uint8_t value[VALUE_SIZE];
}__attribute__((__packed__)) cr_r_rep_big_t;

typedef struct r_rep_message {
  uint64_t l_id;
  uint8_t coalesce_num;
  cr_r_rep_big_t r_rep[MAX_R_REP_COALESCE];
} __attribute__((__packed__)) cr_r_rep_mes_t;


typedef struct cr_r_rep_message_ud_req {
  uint8_t unused[GRH_SIZE];
  uint8_t r_rep_mes[ALIGNED_R_REP_SEND_SIDE];
} cr_r_rep_mes_ud_t;


#endif //ODYSSEY_CR_MESSAGES_H
