//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CRAQ_UTIL_H
#define ODYSSEY_CRAQ_UTIL_H

#include "cr_config.h"
#include "network_context.h"
#include "init_func.h"
#include <cr_inline_util.h>
#include "trace_util.h"

void cr_stats(stats_ctx_t *ctx);

static void cr_static_assert_compile_parameters()
{

  emphatic_print(green, "CRAQ");

}



static void cr_init_functionality(int argc, char *argv[])
{
  generic_static_assert_compile_parameters();
  cr_static_assert_compile_parameters();
  generic_init_globals(QP_NUM);
  handle_program_inputs(argc, argv);
}


static void cr_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[PREP_QP_ID].recv_handler = cr_prepare_handler;
  mfs[PREP_QP_ID].send_helper = cr_send_preps_helper;
  mfs[PREP_QP_ID].insert_helper = cr_insert_prep_help;
  mfs[PREP_QP_ID].recv_kvs = cr_KVS_batch_op_preps;

  mfs[ACK_QP_ID].recv_handler = cr_ack_handler;

  if (ctx->m_id == CR_TAIL_NODE) {
    mfs[R_QP_ID].insert_helper = cr_insert_r_rep_help;
    mfs[R_QP_ID].recv_handler = cr_r_handler;
    mfs[R_QP_ID].send_helper = cr_send_r_reps_helper;
    mfs[R_QP_ID].recv_kvs = cr_KVS_batch_op_reads;
  }
  else {
    mfs[R_QP_ID].insert_helper = cr_insert_read_help;
    mfs[R_QP_ID].recv_handler = cr_r_rep_handler;

  }


  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}


static void cr_init_send_fifos(context_t *ctx)
{

  ctx_ack_mes_t *ack_send_buf = (ctx_ack_mes_t *) ctx->qp_meta[ACK_QP_ID].send_fifo->fifo; //calloc(MACHINE_NUM, sizeof(ctx_ack_mes_t));
  assert(ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size == CTX_ACK_SIZE * MACHINE_NUM);
  memset(ack_send_buf, 0, ctx->qp_meta[ACK_QP_ID].send_fifo->max_byte_size);
  for (int i = 0; i < MACHINE_NUM; i++) {
    ack_send_buf[i].m_id = (uint8_t) machine_id;
    ack_send_buf[i].opcode = OP_ACK;
  }
  //
  for (int fifo_i = 0; fifo_i < ctx->qp_meta[PREP_QP_ID].send_fifo_num; ++fifo_i) {
    cr_prep_mes_t *preps = (cr_prep_mes_t *) ctx->qp_meta[PREP_QP_ID].send_fifo[fifo_i].fifo;
    for (int i = 0; i < CR_PREP_FIFO_SIZE; i++) {

      ctx->qp_meta[PREP_QP_ID].send_fifo[fifo_i].slot_meta[i].rm_id =
        fifo_i == CHAIN_PREP_FIFO_ID ?
        (uint8_t) (ctx->m_id + 1) :  (uint8_t) CR_HEAD_NODE;
      preps[i].opcode = KVS_OP_PUT;
      preps[i].m_id = ctx->m_id;
      for (uint16_t j = 0; j < PREP_COALESCE; j++) {
      }
    }
  }

  if (ctx->m_id != CR_TAIL_NODE) {
    cr_r_mes_t *r_mes = (cr_r_mes_t *) ctx->qp_meta[R_QP_ID].send_fifo->fifo;
    for (int i = 0; i < R_FIFO_SIZE; i++) {

      ctx->qp_meta[R_QP_ID].send_fifo->slot_meta[i].rm_id =
        (uint8_t) CR_TAIL_NODE;
      r_mes[i].m_id = ctx->m_id;
      for (uint16_t j = 0; j < R_COALESCE; j++) {
        r_mes[i].read[j].opcode = KVS_OP_GET;
      }
    }
  }

}


static void cr_init_qp_meta(context_t *ctx)
{
  per_qp_meta_t *qp_meta = ctx->qp_meta;
///
  create_per_qp_meta(&qp_meta[PREP_QP_ID], MAX_PREP_WRS,
                     MAX_RECV_PREP_WRS, SEND_UNI_REQ_RECV_UNI_REQ, RECV_REQ,
                     ACK_QP_ID,
                     2, 2, PREP_BUF_SLOTS,
                     sizeof(cr_prep_mes_ud_t), sizeof(cr_prep_mes_t), false, false,
                     0, 0, CR_PREP_FIFO_SIZE,
                     0, CR_PREP_MES_HEADER,
                     "send preps", "recv preps");
////
////
  create_ack_qp_meta(&qp_meta[ACK_QP_ID],
                     PREP_QP_ID, REM_MACH_NUM,
                     REM_MACH_NUM, 5);

  if (ctx->m_id == CR_TAIL_NODE)
    create_per_qp_meta(&qp_meta[R_QP_ID], CR_R_REP_WRS,
                       CR_RECV_R_WRS, SEND_UNI_REP_LDR_RECV_UNI_REQ, RECV_REQ,
                       R_QP_ID,
                       REM_MACH_NUM, REM_MACH_NUM, CR_R_BUF_SLOTS,
                       sizeof(cr_r_mes_ud_t), sizeof(cr_r_rep_mes_t), false, false,
                       0, CR_TAIL_NODE, R_REP_FIFO_SIZE, 0, R_REP_MES_HEADER,
                       "send r_Reps", "recv reads");
  else
    create_per_qp_meta(&qp_meta[R_QP_ID], CR_R_WRS,
                       CR_RECV_R_REP_WRS, SEND_UNI_REQ_RECV_LDR_REP, RECV_REPLY,
                       R_QP_ID, 1, 1, CR_RECV_R_REP_WRS,
                       sizeof(cr_r_rep_mes_ud_t), sizeof(cr_r_mes_t), false, false,
                       0, CR_TAIL_NODE, R_FIFO_SIZE,
                       0, CR_R_MES_HEADER,
                       "send reads", "recv read_replies");



  cr_qp_meta_mfs(ctx);
  cr_init_send_fifos(ctx);

}

static void* set_up_cr_ctx(context_t *ctx)
{
  cr_ctx_t* cr_ctx = (cr_ctx_t*) calloc(1,sizeof(cr_ctx_t));

  cr_ctx->w_rob = fifo_constructor(CR_W_ROB_SIZE, sizeof(cr_w_rob_t), false, 0, 1);

  cr_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));
  cr_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));


  cr_ctx->ops = (ctx_trace_op_t *) calloc((size_t) CR_TRACE_BATCH, sizeof(ctx_trace_op_t));

  cr_ctx->buf_reads = fifo_constructor(2 * SESSIONS_PER_THREAD, sizeof(cr_buf_op_t), false, 0, 1);

  for (int i = 0; i < SESSIONS_PER_THREAD; i++) cr_ctx->stalled[i] = false;
    for (uint32_t i = 0; i < CR_W_ROB_SIZE; i++) {
      cr_w_rob_t *w_rob = get_fifo_slot(cr_ctx->w_rob, i);
      w_rob->w_state = INVALID;
      w_rob->id = (uint16_t) i;
  }
  cr_ctx->prev_node = (uint8_t) (ctx->m_id - 1);
  cr_ctx->next_node = (uint8_t) (ctx->m_id + 1);
  cr_ctx->ptrs_to_ops = calloc(1, sizeof(cr_ptrs_to_op_t));
  cr_ctx->ptrs_to_ops->ops = calloc(CR_MAX_INCOMING_PREP, sizeof(void*));
  cr_ctx->ptrs_to_ops->ptr_to_mes = calloc(CR_MAX_INCOMING_PREP, sizeof(void*));
  cr_ctx->ptrs_to_ops->coalesce = calloc(CR_MAX_INCOMING_PREP, sizeof(bool));
  if (ctx->m_id != CR_TAIL_NODE)
    cr_ctx->r_rob = fifo_constructor(SESSIONS_PER_THREAD,
                                     sizeof(cr_r_rob_t), false, 0, 1);

  if (!ENABLE_CLIENTS)
    cr_ctx->trace = trace_init(ctx->t_id);

  return (void *) cr_ctx;
}

typedef struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double prep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
  double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_prep[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


  double cache_hits_per_thread[WORKERS_PER_MACHINE];


  double preps_sent[WORKERS_PER_MACHINE];
  double acks_sent[WORKERS_PER_MACHINE];
  double coms_sent[WORKERS_PER_MACHINE];

  double received_coms[WORKERS_PER_MACHINE];
  double received_acks[WORKERS_PER_MACHINE];
  double received_preps[WORKERS_PER_MACHINE];

  double write_ratio_per_client[WORKERS_PER_MACHINE];
} all_stats_t;

#endif //ODYSSEY_CRAQ_UTIL_H
