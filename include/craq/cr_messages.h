//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_MESSAGES_H
#define ODYSSEY_CR_MESSAGES_H


/// PREP_QP_ID
//#define PREP_CREDITS 10
#define PREP_COALESCE 16
#define COM_CREDITS 80
#define CR_W_COALESCE 16
//#define MAX_PREP_SIZE 500

#define MAX_PREP_WRS (1 + ((MACHINE_NUM * SESSIONS_PER_THREAD ) / PREP_COALESCE))




#define CR_PREP_MES_HEADER 12 // opcode(1), coalesce_num(1) l_id (8)
//#define EFFECTIVE_MAX_PREP_SIZE (MAX_PREP_SIZE - PREP_MES_HEADER)
//#define PREP_SIZE (20 + VALUE_SIZE)
////#define PREP_COALESCE (EFFECTIVE_MAX_PREP_SIZE / PREP_SIZE)
//#define PREP_SEND_SIZE (PREP_MES_HEADER + (PREP_COALESCE * PREP_SIZE))
//#define PREP_RECV_SIZE (GRH_SIZE + PREP_SEND_SIZE)




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



//#define COM_WRS MESSAGES_IN_BCAST_BATCH
//#define RECV_COM_WRS (REM_MACH_NUM * COM_CREDITS)
//#define COM_BUF_SLOTS RECV_COM_WRS
//#define MAX_LIDS_IN_A_COMMIT SESSIONS_PER_THREAD
//
//
//
//#define CR_MAX_W_WRS (MACHINE_NUM + (SESSIONS_PER_THREAD / CR_W_COALESCE))
//#define CR_MAX_RECV_W_WRS ( REM_MACH_NUM * SESSIONS_PER_THREAD)
//#define CR_BUF_SLOTS  CR_MAX_RECV_W_WRS
//#define CR_W_HEADER 4
//
//typedef struct cr_write {
//  mica_key_t key;	/* 8B */
//  uint16_t sess_id;
//  uint8_t value[VALUE_SIZE];
//} __attribute__((__packed__)) cr_write_t;
//
//typedef struct cr_w_message {
//  uint8_t coalesce_num;
//  uint8_t opcode;
//  uint8_t m_id;
//  uint8_t unused;
//  cr_write_t write[CR_W_COALESCE];
//} __attribute__((__packed__)) cr_w_mes_t;
//
//
//typedef struct cr_w_message_ud_req {
//  uint8_t unused[GRH_SIZE];
//  cr_w_mes_t w_mes;
//} cr_w_mes_ud_t;
//
//
//#define CR_W_SIZE sizeof(cr_write_t)

#endif //ODYSSEY_CR_MESSAGES_H
