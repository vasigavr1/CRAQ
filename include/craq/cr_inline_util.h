//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_INLINE_UTIL_H
#define ODYSSEY_CR_INLINE_UTIL_H

#include <netw_func.h>
#include "cr_debug_util.h"
#include "cr_kvs_util.h"


static inline uint8_t get_key_owner(context_t *ctx,
                                    mica_key_t key)
{
  return CR_HEAD_NODE;
}

static inline uint8_t get_fifo_i(context_t *ctx,
                                 uint8_t rm_id)
{
  return rm_id > ctx->m_id ? (uint8_t) (rm_id - 1) : rm_id;
}

static inline bool filter_remote_writes(context_t *ctx,
                                        ctx_trace_op_t *op)
{
  if (op->opcode != KVS_OP_PUT) return false;

  uint8_t rm_id = get_key_owner(ctx, op->key);
  if (rm_id == ctx->m_id) return false;
  else {
    //ctx_insert_mes(ctx, W_QP_ID, (uint32_t) CR_W_SIZE, 1, false,
    //               op, NOT_USED, get_fifo_i(ctx, rm_id));
  }
  return true;
}

static inline void cr_batch_from_trace_to_KVS(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = cr_ctx->ops;
  trace_t *trace = cr_ctx->trace;

  uint16_t kvs_op_i = 0, op_num = 0;
  int working_session = -1;

  if (all_sessions_are_stalled(ctx, cr_ctx->all_sessions_stalled,
                               &cr_ctx->stalled_sessions_dbg_counter))
    return;
  if (!find_starting_session(ctx, cr_ctx->last_session,
                             cr_ctx->stalled, &working_session)) return;

  bool passed_over_all_sessions = false;

  /// main loop
  while (kvs_op_i < CR_TRACE_BATCH && !passed_over_all_sessions) {

    ctx_fill_trace_op(ctx, &trace[cr_ctx->trace_iter], &ops[kvs_op_i], working_session);
    cr_ctx->stalled[working_session] = true;
    passed_over_all_sessions =
      ctx_find_next_working_session(ctx, &working_session,
                                    cr_ctx->stalled,
                                    cr_ctx->last_session,
                                    &cr_ctx->all_sessions_stalled);
    if (!ENABLE_CLIENTS) {
      cr_ctx->trace_iter++;
      if (trace[cr_ctx->trace_iter].opcode == NOP) cr_ctx->trace_iter = 0;
    }

    if (!filter_remote_writes(ctx, &ops[kvs_op_i]))
      kvs_op_i++;
    op_num++;
  }
  cr_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].cache_hits_per_thread += op_num;
  if (kvs_op_i > 0) cr_KVS_batch_op_trace(ctx, kvs_op_i);
}

///* ---------------------------------------------------------------------------
////------------------------------ COMMIT WRITES -----------------------------
////---------------------------------------------------------------------------*/

static inline void cr_apply_writes(context_t *ctx,
                                   uint32_t pull_ptr,
                                   uint32_t write_num)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;


  for (int w_i = 0; w_i < write_num; ++w_i) {
    cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_slot_mod(cr_ctx->w_rob, pull_ptr + w_i);
    if (ENABLE_ASSERTIONS) {
      assert(w_rob->version > 0);
      assert(w_rob != NULL);
      assert(w_rob->w_state = SEMI_INVALID);
    }
    w_rob->w_state = INVALID;
    mica_op_t *kv_ptr = w_rob->kv_ptr;
    lock_seqlock(&kv_ptr->seqlock);
    {
      if (ENABLE_ASSERTIONS) assert(kv_ptr->version > 0);
      if (kv_ptr->version == w_rob->version) {
        if (ENABLE_ASSERTIONS)
          assert(kv_ptr->state == CR_INV);
        kv_ptr->state = CR_V;
      }
    }
    unlock_seqlock(&kv_ptr->seqlock);
  }
}

static inline void cr_complete_local_write(context_t * ctx,
                                           cr_w_rob_t *w_rob)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  uint16_t sess_id = w_rob->sess_id;
  if (ENABLE_ASSERTIONS) {
    assert(ctx->m_id == CR_HEAD_NODE);
    assert(sess_id < SESSIONS_PER_THREAD);
    if (!cr_ctx->stalled[sess_id]) {
      if (DEBUG_WRITES)
        my_printf(red, "Session not stalled: owner %u/%u \n",
                  w_rob->owner_m_id, w_rob->sess_id);
      assert(false);
    }
    else {
      if (DEBUG_WRITES)
        my_printf(green, "Session is stalled: owner %u/%u \n",
                  w_rob->owner_m_id, w_rob->sess_id);
    }
  }
  cr_ctx->all_sessions_stalled = false;
  signal_completion_to_client(sess_id,
                              cr_ctx->index_to_req_array[sess_id],
                              ctx->t_id);
  cr_ctx->stalled[sess_id] = false;
}

static inline void cr_commit_writes(context_t *ctx)
{
  uint16_t write_num = 0;
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;

  cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_pull_slot(cr_ctx->w_rob);
  uint32_t starting_pull_ptr = cr_ctx->w_rob->pull_ptr;
  while (w_rob->w_state == READY) {
    
    w_rob->w_state = SEMI_INVALID;
    __builtin_prefetch(&w_rob->kv_ptr->seqlock, 0, 0);
    //my_printf(green, "Commit sess %u write %lu, version: %lu \n",
    //          w_rob->sess_id, cr_ctx->committed_w_id + write_num, w_rob->version);

    if (w_rob->owner_m_id == ctx->m_id) 
      cr_complete_local_write(ctx, w_rob);

    fifo_incr_pull_ptr(cr_ctx->w_rob);
    fifo_decrem_capacity(cr_ctx->w_rob);
    w_rob = (cr_w_rob_t *) get_fifo_pull_slot(cr_ctx->w_rob);
    write_num++;
  }
  

  if (write_num > 0) {
    cr_ctx->ptrs_to_ops->op_num = write_num;
    cr_apply_writes(ctx, starting_pull_ptr, write_num);
  }
}


///* ---------------------------------------------------------------------------
////------------------------------INSERT HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void cr_fill_w_rob(context_t *ctx,
                                 cr_prep_t *prep,
                                 cr_w_rob_t *w_rob)
{
  if (ENABLE_ASSERTIONS) {
    assert(w_rob->w_state == SEMIVALID);
  }
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  w_rob->w_state = VALID;
  w_rob->sess_id = prep->sess_id;
  w_rob->l_id = cr_ctx->inserted_w_id;

  if (DEBUG_WRITES)
    my_printf(cyan, "W_rob insert sess %u write %lu, w_rob_i %u\n",
              w_rob->sess_id, w_rob->l_id,
              cr_ctx->w_rob->push_ptr);

}

static inline void cr_fill_prep_and_w_rob(context_t *ctx,
                                          cr_prep_t *prep, void *source,
                                          cr_w_rob_t *w_rob,
                                          source_t source_flag)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  cr_write_t *write = (cr_write_t *) source;
  switch (source_flag) {
    case LOCAL_PREP:
      if (ENABLE_ASSERTIONS) {
        assert (prep->m_id == ctx->m_id);
        assert(cr_ctx->stalled[op->session_id]);
      }
      prep->version = w_rob->version;
      prep->m_id = w_rob->owner_m_id;
      prep->key = op->key;
      memcpy(prep->value, op->value_to_write, (size_t) VALUE_SIZE);
      prep->sess_id = op->session_id;

      cr_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
      break;
    case REMOTE_WRITE:
      if (ENABLE_ASSERTIONS) assert (prep->m_id != ctx->m_id);
      prep->version = w_rob->version;
      prep->m_id = w_rob->owner_m_id;
      prep->key = write->key;
      memcpy(prep->value, write->value, (size_t) VALUE_SIZE);
      prep->sess_id = write->sess_id;
      break;
    case REMOTE_PREP:
      memcpy(prep, source, sizeof(cr_prep_t));
      w_rob->version = prep->version;
      if (ENABLE_ASSERTIONS) assert(w_rob->version > 0);
      break;
    case NOT_USED:
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
  cr_fill_w_rob(ctx, prep, w_rob);
}


static inline void cr_insert_prep_help(context_t *ctx, void* prep_ptr,
                                       void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  cr_prep_t *prep = (cr_prep_t *) prep_ptr;

  fifo_t *working_fifo = cr_ctx->w_rob;
  cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_push_slot(working_fifo);

  cr_fill_prep_and_w_rob(ctx, prep, source, w_rob, (source_t) source_flag);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  if (ENABLE_ASSERTIONS) assert(slot_meta->rm_id == ctx->m_id + 1);
  cr_prep_mes_t *prep_mes = (cr_prep_mes_t *) get_fifo_push_slot(send_fifo);
  prep_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = cr_ctx->inserted_w_id;
    fifo_set_push_backward_ptr(send_fifo, working_fifo->push_ptr);
  }

  if (ctx->m_id == CR_HEAD_NODE) fifo_increm_capacity(working_fifo);
  fifo_incr_push_ptr(working_fifo);
  cr_ctx->inserted_w_id++;
}


///* ---------------------------------------------------------------------------
////------------------------------SEND HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void cr_send_preps_helper(context_t *ctx)
{
  cr_checks_and_stats_on_bcasting_preps(ctx);
  //if (ctx->m_id == CR_HEAD_NODE) ctx_refill_recvs(ctx, W_QP_ID);
}

///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/


static inline bool cr_prepare_handler(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cr_prep_mes_ud_t *incoming_preps = (volatile cr_prep_mes_ud_t *) recv_fifo->fifo;
  cr_prep_mes_t *prep_mes = (cr_prep_mes_t *) &incoming_preps[recv_fifo->pull_ptr].prepare;

  uint8_t coalesce_num = prep_mes->coalesce_num;

  fifo_t *w_rob_fifo = cr_ctx->w_rob;
  bool preps_fit_in_w_rob =
    w_rob_fifo->capacity + coalesce_num <= CR_W_ROB_SIZE;

  if (!preps_fit_in_w_rob) return false;

  if (ctx->m_id != CR_TAIL_NODE)
    fifo_increase_capacity(w_rob_fifo, coalesce_num);

  cr_check_polled_prep_and_print(ctx, prep_mes);

  if (ctx->m_id == CR_TAIL_NODE)
    ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  prep_mes->l_id, cr_ctx->prev_node);

  cr_ptrs_to_op_t *ptrs_to_prep = cr_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prep->op_num = 0;

  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    cr_check_w_rob_when_handling_a_prep(ctx, ptrs_to_prep,
                                        w_rob_fifo,
                                        prep_mes, prep_i);

    cr_check_prepare_and_print(ctx, prep_mes, prep_i);
    ptrs_to_prep->ops[ptrs_to_prep->op_num] = &prep_mes->prepare[prep_i];
    ptrs_to_prep->ptr_to_mes[ptrs_to_prep->op_num] = prep_mes;
    ptrs_to_prep->op_num++;
  }

  if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;

  return true;
}

static inline bool cr_ack_handler(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
  uint32_t ack_num = ack->ack_num;
  uint64_t l_id = ack->l_id;
  uint64_t pull_lid = cr_ctx->committed_w_id; // l_id at the pull pointer
  uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added

  //ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  //per_qp_meta_t *com_qp_meta = &ctx->qp_meta[COM_QP_ID];
  //com_qp_meta->credits[ack->m_id] = com_qp_meta->max_credits;

  if (ENABLE_ASSERTIONS) {
    if ((cr_ctx->w_rob->capacity == 0) ||
        (pull_lid >= l_id && (pull_lid - l_id) >= ack_num)) {
      assert(false);
      return true;
    }
  }
  //printf("Receiving %u acks for %lu \n", ack->ack_num, ack->l_id);
  if (ctx->m_id != CR_TAIL_NODE && ctx->m_id != CR_HEAD_NODE)
    ctx_ack_insert(ctx, ACK_QP_ID, ack->ack_num,  ack->l_id, cr_ctx->prev_node);

  //ack_ptr = ctx_find_when_the_ack_points_acked(ack, cr_ctx->w_rob,
  //                                             pull_lid, &ack_num);

  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {
    cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_slot_mod(cr_ctx->w_rob, l_id + ack_i);
    if (ENABLE_ASSERTIONS) {
      if (w_rob->w_state != SENT) {
        printf("Wrob_state %u w_rob id %u w_rob l_id %lu/%lu, ack_i %u, committed id %lu\n",
               w_rob->w_state, w_rob->id, w_rob->l_id, ack->l_id + ack_i,
               ack_i, cr_ctx->committed_w_id);
        assert(false);
      }
      assert(w_rob->l_id == l_id + ack_i);
    }
    w_rob->w_state = READY;
  }
  cr_ctx->committed_w_id += ack_num;

  return true;
}

///* ---------------------------------------------------------------------------
////------------------------------ MAIN LOOP -----------------------------
////---------------------------------------------------------------------------*/


static inline void cr_main_loop(context_t *ctx)
{
  if (ctx->t_id == 0) my_printf(yellow, "CR main loop \n");


  while(true) {

    cr_batch_from_trace_to_KVS(ctx);
    //
    //ctx_send_unicasts(ctx, W_QP_ID);
    //
    //ctx_poll_incoming_messages(ctx, W_QP_ID);
    //
    ctx_send_unicasts(ctx, PREP_QP_ID);
    //
    //
    ctx_poll_incoming_messages(ctx, PREP_QP_ID);
    //
    ctx_send_acks(ctx, ACK_QP_ID);
    //
    ctx_poll_incoming_messages(ctx, ACK_QP_ID);
    //
    //ctx_poll_incoming_messages(ctx, COM_QP_ID);
    //
    if (ctx->m_id != CR_TAIL_NODE)
      cr_commit_writes(ctx);
    //
    //ctx_send_broadcasts(ctx, COM_QP_ID);
  }
}

#endif //ODYSSEY_CR_INLINE_UTIL_H
