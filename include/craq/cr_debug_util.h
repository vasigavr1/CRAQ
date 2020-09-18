//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_DEBUG_UTIL_H
#define ODYSSEY_CR_DEBUG_UTIL_H

#include "cr_config.h"

static inline void cr_checks_and_stats_on_bcasting_preps(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];

  bool steer_to_head = ctx->ctx_tmp->counter == STEER_TO_HEAD_FIFO_ID;
  uint8_t fifo_i = steer_to_head ?
                   (uint8_t) STEER_TO_HEAD_FIFO_ID :
                   (uint8_t) CHAIN_PREP_FIFO_ID;
  fifo_t *send_fifo = &qp_meta->send_fifo[fifo_i];

  // Create the broadcast messages
  cr_prep_mes_t *prep_buf = (cr_prep_mes_t *) send_fifo->fifo;
  cr_prep_mes_t *prep_mes = &prep_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
    assert(prep_mes->coalesce_num == (uint8_t) slot_meta->coalesce_num);

    if (DEBUG_PREPARES)
      printf("Wrkr %d has %u prep_mes bcasts to send\n", ctx->t_id,
             send_fifo->net_capacity);

    uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);
    for (uint16_t i = 0; i < coalesce_num; i++) {
      if (!steer_to_head) {
        cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_slot_mod(cr_ctx->w_rob, backward_ptr + i);
        if (ENABLE_ASSERTIONS) assert(w_rob->w_state == VALID);
        w_rob->w_state = SENT;
      }
      if (DEBUG_PREPARES)
        printf("prep_mes %d, total message capacity %d\n",
               i,  slot_meta->byte_size);
    }

    if (DEBUG_PREPARES)
      my_printf(green, "Wrkr %d : I BROADCAST a prep_mes message %d of "
                  "%u preps with total w_size %u,  lid: %lu  \n",
                ctx->t_id, prep_mes->opcode, coalesce_num, slot_meta->byte_size,
                 prep_mes->l_id);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].preps_sent +=
      coalesce_num;
    t_stats[ctx->t_id].prep_sent_mes_num++;
  }
}




static inline void cr_check_polled_prep_and_print(context_t *ctx,
                                                   cr_prep_mes_t* prep_mes)
{

  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (DEBUG_PREPARES)
    my_printf(green, "Wrkr %d sees a prep_mes message "
                "with %d prepares at index %u l_id %u \n",
              ctx->t_id, prep_mes->coalesce_num, recv_fifo->pull_ptr,
              prep_mes->l_id);
  if (ENABLE_ASSERTIONS) {
    assert(prep_mes->opcode == KVS_OP_PUT);
    assert(prep_mes->coalesce_num > 0 && prep_mes->coalesce_num <= PREP_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_preps += prep_mes->coalesce_num;
    t_stats[ctx->t_id].received_preps_mes_num++;
  }
}


static inline void cr_check_prepare_and_print(context_t *ctx,
                                               cr_prep_mes_t *prep_mes,
                                               uint8_t prep_i)
{
  if (ENABLE_ASSERTIONS) {
    cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
    cr_prep_t *prepare = &prep_mes->prepare[prep_i];

    if (DEBUG_PREPARES)
      my_printf(green, "Wrkr %u, prep_i %u new write from m_id %u \n",
                ctx->t_id, prep_i, prep_mes->m_id);
  }
}

static inline void cr_check_w_rob_when_handling_a_prep(context_t *ctx,
                                                       cr_ptrs_to_op_t *ptrs_to_prep,
                                                       fifo_t *w_rob_fifo,
                                                       cr_prep_mes_t *prep_mes,
                                                       uint8_t prep_i)
{
  if (ENABLE_ASSERTIONS) {
    cr_prep_t *prep= &prep_mes->prepare[prep_i];
    cr_w_rob_t *w_rob = (cr_w_rob_t *)
      get_fifo_push_relative_slot(w_rob_fifo, prep_i);
    assert(w_rob->w_state == INVALID);
    w_rob->l_id = prep_mes->l_id + prep_i;
    assert(ptrs_to_prep->op_num < CR_MAX_INCOMING_PREP);

    cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;

    if (prep->m_id == ctx->m_id)
      assert(cr_ctx->stalled[prep->sess_id]);
  }
}
#endif //ODYSSEY_CR_DEBUG_UTIL_H
