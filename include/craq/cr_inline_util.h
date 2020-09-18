//
// Created by vasilis on 17/09/20.
//

#ifndef ODYSSEY_CR_INLINE_UTIL_H
#define ODYSSEY_CR_INLINE_UTIL_H

#include <netw_func.h>
#include "cr_debug_util.h"
#include "cr_kvs_util.h"
#include "cr_generic_util.h"


static inline bool filter_remote_writes(context_t *ctx,
                                        ctx_trace_op_t *op)
{
  if (op->opcode != KVS_OP_PUT) return false;

  uint8_t rm_id = get_key_owner(ctx, op->key);
  if (rm_id == ctx->m_id) return false;
  else {
    ctx_insert_mes(ctx, PREP_QP_ID, sizeof(cr_prep_t), 1,
                   false, op, CR_LOCAL_PREP, STEER_TO_HEAD_FIFO_ID);
  }
  return true;
}

static inline uint16_t cr_find_trace_ops(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = cr_ctx->ops;
  trace_t *trace = cr_ctx->trace;

  uint16_t kvs_op_i = 0, op_num = 0;
  int working_session = -1;

  if (all_sessions_are_stalled(ctx, cr_ctx->all_sessions_stalled,
                               &cr_ctx->stalled_sessions_dbg_counter))
    return 0;
  if (!find_starting_session(ctx, cr_ctx->last_session,
                             cr_ctx->stalled, &working_session)) return 0;

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
  return kvs_op_i;
}

static inline void cr_batch_from_trace_to_KVS(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  uint16_t kvs_op_i = cr_find_trace_ops(ctx);
  if (kvs_op_i > 0 || cr_ctx->buf_reads->capacity > 0)
    cr_KVS_batch_op_trace(ctx, kvs_op_i);
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
      assert(w_rob->w_state == SEMI_INVALID);
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



static inline void cr_commit_writes(context_t *ctx)
{
  uint16_t write_num = 0;
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;

  cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_pull_slot(cr_ctx->w_rob);
  uint32_t starting_pull_ptr = cr_ctx->w_rob->pull_ptr;
  while (w_rob->w_state == READY) {
    
    w_rob->w_state = SEMI_INVALID;
    __builtin_prefetch(&w_rob->kv_ptr->seqlock, 0, 0);
    if (w_rob->owner_m_id == ctx->m_id)
      cr_free_session(ctx, w_rob->sess_id);

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


static inline void cr_fill_prep_from_op(context_t *ctx,
                                        cr_prep_t *prep,
                                        ctx_trace_op_t *op)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  prep->m_id = ctx->m_id;
  prep->key = op->key;
  memcpy(prep->value, op->value_to_write, (size_t) VALUE_SIZE);
  prep->sess_id = op->session_id;
  cr_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
}

static inline void cr_fill_prep_and_w_rob(context_t *ctx,
                                          cr_prep_t *prep, void *source,
                                          cr_w_rob_t *w_rob,
                                          source_t source_flag)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  switch (source_flag) {
    case CR_LOCAL_PREP:
      prep->version = w_rob->version;
      cr_fill_prep_from_op(ctx, prep, op);
      w_rob->owner_m_id = CR_HEAD_NODE;
      if (ENABLE_ASSERTIONS) {
        assert(is_head(ctx));
        assert (prep->m_id == ctx->m_id);
        assert(cr_ctx->stalled[op->session_id]);
      }
      break;
    case STEERED_PREP: // HEAD handles a steered prep
      if (ENABLE_ASSERTIONS) assert(is_head(ctx));
      memcpy(prep, source, sizeof(cr_prep_t));
      prep->version = w_rob->version;
      w_rob->owner_m_id = prep->m_id;
      break;
    case CHAIN_PREP: //  Middle nodes propagate preps
      if (ENABLE_ASSERTIONS) assert(is_middle(ctx));
      memcpy(prep, source, sizeof(cr_prep_t));
      w_rob->version = prep->version;
      w_rob->owner_m_id = prep->m_id;
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
  bool steer_to_head = source_flag == CR_LOCAL_PREP && !is_head(ctx);

  uint8_t fifo_i = steer_to_head ?
                   (uint8_t) STEER_TO_HEAD_FIFO_ID : (uint8_t) CHAIN_PREP_FIFO_ID;

  fifo_t *send_fifo = &qp_meta->send_fifo[fifo_i];

  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  cr_prep_t *prep = (cr_prep_t *) prep_ptr;

  fifo_t *working_fifo = cr_ctx->w_rob;
  cr_w_rob_t *w_rob = (cr_w_rob_t *) get_fifo_push_slot(working_fifo);
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);

  if (steer_to_head) {
    cr_fill_prep_from_op(ctx, prep, source);
    if (ENABLE_ASSERTIONS) {
      assert(slot_meta->rm_id == CR_HEAD_NODE);
      assert(!is_head(ctx));
    }
  }
  else {
    cr_fill_prep_and_w_rob(ctx, prep, source, w_rob, (source_t) source_flag);
    if (ENABLE_ASSERTIONS) {
      assert(slot_meta->rm_id == ctx->m_id + 1);
      assert(ctx->m_id != CR_TAIL_NODE);
    }
  }

  cr_prep_mes_t *prep_mes = (cr_prep_mes_t *) get_fifo_push_slot(send_fifo);
  prep_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = cr_ctx->inserted_w_id;
    if (!steer_to_head) fifo_set_push_backward_ptr(send_fifo, working_fifo->push_ptr);
  }

  if (!steer_to_head) {
    if (source_flag == CR_LOCAL_PREP) fifo_increm_capacity(working_fifo);
    fifo_incr_push_ptr(working_fifo);
    cr_ctx->inserted_w_id++;
  }
}


static inline void cr_insert_read_help(context_t *ctx, void *r_ptr,
                                       void *source, uint32_t source_flag)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  cr_r_rob_t *r_rob = (cr_r_rob_t *) get_fifo_push_slot(cr_ctx->r_rob);
  ctx_trace_op_t *op = source;
  cr_read_t *read = (cr_read_t *) r_ptr;

  read->version = r_rob->version;
  read->key = op->key;

  r_rob->value_to_read = op->value_to_read;
  r_rob->sess_id = op->session_id;
  if (ENABLE_ASSERTIONS) {
    assert(cr_ctx->stalled[r_rob->sess_id]);
    assert(r_rob->state == INVALID);
  }
  r_rob->state = VALID;
  r_rob->l_id = cr_ctx->inserted_r_id;

  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t* send_fifo = qp_meta->send_fifo;
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  cr_r_mes_t *r_mes = (cr_r_mes_t *) get_fifo_push_slot(send_fifo);
  if (ENABLE_ASSERTIONS) {
    assert(r_ptr == (void *)&r_mes->read[slot_meta->coalesce_num - 1]);
  }

  if (slot_meta->coalesce_num == 1) {
    r_mes->l_id = r_rob->l_id;
    fifo_set_push_backward_ptr(send_fifo, cr_ctx->r_rob->push_ptr);
  }
  r_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  
  fifo_incr_push_ptr(cr_ctx->r_rob);
  fifo_increm_capacity(cr_ctx->r_rob);
  cr_ctx->inserted_r_id++;
}

static inline void cr_fill_r_rep(cr_read_t *read, 
                                 cr_r_rep_big_t *r_rep, 
                                 mica_op_t *kv_ptr, 
                                 uint16_t t_id)
{
  r_rep->opcode = VERSION_DIFF;
  uint32_t debug_cntr = 0;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "filling r_rep", t_id);
    if (read->version == kv_ptr->version) r_rep->opcode = VERSION_EQUAL;
    else {
      memcpy(r_rep->value, kv_ptr->value, (size_t) VALUE_SIZE);
      r_rep->version = kv_ptr->version;
    }
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));
}

static inline void cr_insert_r_rep_help(context_t *ctx, void *r_rep_ptr,
                                        void *source, uint32_t op_i)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cr_ptrs_to_op_t *ptrs_to_r = cr_ctx->ptrs_to_ops;
  cr_r_mes_t *r_mes = cr_ctx->ptrs_to_ops->ptr_to_mes[op_i];
  cr_read_t *read = ptrs_to_r->ops[op_i];


  cr_r_rep_big_t *r_rep = (cr_r_rep_big_t *) r_rep_ptr;

  cr_fill_r_rep(read, r_rep, (mica_op_t *) source, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  if (r_rep->opcode == VERSION_DIFF)
    slot_meta->byte_size += (R_REP_BIG_SIZE - R_REP_SMALL_SIZE);

  cr_r_rep_mes_t *r_rep_mes = (cr_r_rep_mes_t *) get_fifo_push_slot(send_fifo);
  if (slot_meta->coalesce_num == 1) {
    r_rep_mes->l_id = r_mes->l_id;
    slot_meta->rm_id = r_mes->m_id;
  }
  r_rep_mes->coalesce_num = (uint8_t) slot_meta->coalesce_num;
}

///* ---------------------------------------------------------------------------
////------------------------------SEND HELPERS -----------------------------
////---------------------------------------------------------------------------*/

static inline void cr_send_preps_helper(context_t *ctx)
{
  cr_checks_and_stats_on_bcasting_preps(ctx);
  if (is_head(ctx)) ctx_refill_recvs(ctx, PREP_QP_ID);
}

static inline void cr_send_r_reps_helper(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  cr_r_rep_mes_t *r_rep_mes = (cr_r_rep_mes_t *) get_fifo_pull_slot(send_fifo);
  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);

  if (DEBUG_READ_REPS)
    my_printf(yellow, "Wrkr %u SENDING R_REP: coalesce_num %u, l_id %lu, to m_id %u, size %lu address %p\n",
              ctx->t_id, r_rep_mes->coalesce_num, r_rep_mes->l_id,
              slot_meta->rm_id, slot_meta->byte_size, (void *) r_rep_mes);
  uint16_t byte_ptr = R_REP_MES_HEADER;
  for (int i = 0; i < r_rep_mes->coalesce_num; ++i) {
    cr_r_rep_big_t *r_rep = (cr_r_rep_big_t *) (((void *) r_rep_mes) + byte_ptr);

    if (DEBUG_READ_REPS)
      my_printf(green, "Wrkr: Sending %u R_rep %u/%u opcode %u, byte_ptr %u, address %p\n",
                ctx->t_id, i, r_rep_mes->coalesce_num, r_rep->opcode, byte_ptr, (void*)r_rep);

    if (r_rep->opcode == VERSION_DIFF) {
      byte_ptr += R_REP_BIG_SIZE;
    }
    else byte_ptr += R_REP_SMALL_SIZE;
  }


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

  if (!is_tail(ctx))
    fifo_increase_capacity(w_rob_fifo, coalesce_num);

  cr_check_polled_prep_and_print(ctx, prep_mes);

  if (is_tail(ctx))
    ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  prep_mes->l_id, cr_ctx->prev_node);

  cr_ptrs_to_op_t *ptrs_to_prep = cr_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_prep->op_num = 0;

  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    cr_check_w_rob_when_handling_a_prep(ctx, ptrs_to_prep,
                                        w_rob_fifo,
                                        prep_mes, prep_i);

    cr_check_prepare_and_print(ctx, prep_mes, prep_i);
    ptrs_to_prep->ops[ptrs_to_prep->op_num] = &prep_mes->prepare[prep_i];
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


  if (ENABLE_ASSERTIONS) {
    if ((cr_ctx->w_rob->capacity == 0) ||
        (pull_lid >= l_id && (pull_lid - l_id) >= ack_num)) {
      assert(false);
      return true;
    }
  }
  //printf("Receiving %u acks for %lu \n", ack->ack_num, ack->l_id);
  if (is_middle(ctx))
    ctx_ack_insert(ctx, ACK_QP_ID, ack->ack_num,  ack->l_id, cr_ctx->prev_node);

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

static inline bool cr_r_handler(context_t *ctx)
{

  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;

  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cr_r_mes_ud_t *incoming_reads = (volatile cr_r_mes_ud_t *) recv_fifo->fifo;
  cr_r_mes_t *r_mes = (cr_r_mes_t *) &incoming_reads[recv_fifo->pull_ptr].r_mes;
  uint8_t r_num = r_mes->coalesce_num;

  if (DEBUG_READS)
    my_printf(green, "WRKR %u RECEIVING READ MESSAGE: lid %u, coalesce num %u from %u \n",
              ctx->t_id, r_mes->l_id, r_mes->coalesce_num, r_mes->m_id);

  cr_ptrs_to_op_t *ptrs_to_r = cr_ctx->ptrs_to_ops;
  if (qp_meta->polled_messages == 0) ptrs_to_r->op_num = 0;
  
  for (uint16_t r_i = 0; r_i < r_num; r_i++) {
    cr_read_t *read = &r_mes->read[r_i];
    //printf("Receiving read opcode %u \n", read->opcode);

    if (DEBUG_READS)
      my_printf(yellow, "wrkr %u Read %u, opcode %u, version %lu, key.bkt %u\n",
                ctx->t_id, r_i, read->opcode, read->version, read->key.bkt);

    if (ENABLE_ASSERTIONS) {
      assert(read->opcode == KVS_OP_GET);
      assert(read->key.bkt > 0);
    }
    ptrs_to_r->ops[ptrs_to_r->op_num] = read;
    ptrs_to_r->ptr_to_mes[ptrs_to_r->op_num] = r_mes;
    ptrs_to_r->coalesce[ptrs_to_r->op_num] = r_i > 0;
    ptrs_to_r->op_num++;
  }

  return true;
}

static inline bool cr_r_rep_handler(context_t *ctx)
{
  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile cr_r_rep_mes_ud_t *incoming_r_reps =
    (volatile cr_r_rep_mes_ud_t *) recv_fifo->fifo;
  cr_r_rep_mes_t *r_rep_mes = (cr_r_rep_mes_t *)
    &incoming_r_reps[recv_fifo->pull_ptr].r_rep_mes;
  fifo_t *r_rob_fifo = cr_ctx->r_rob;
  if (DEBUG_READ_REPS)
    my_printf(cyan, "WRKR %u: RECEIVING R_REP: l_id %u/%lu, coalesce_num %u \n",
              ctx->t_id, r_rep_mes->l_id, cr_ctx->inserted_r_id, r_rep_mes->coalesce_num);
  if (ENABLE_ASSERTIONS) {
    assert(r_rob_fifo->capacity > 0);
    assert(r_rep_mes->l_id == cr_ctx->committed_r_id);
  }

  uint16_t byte_ptr = R_REP_MES_HEADER;
  for (int r_rep_i = 0; r_rep_i < r_rep_mes->coalesce_num; ++r_rep_i) {
    cr_r_rob_t *r_rob = (cr_r_rob_t *) get_fifo_pull_slot(r_rob_fifo);
    cr_r_rep_big_t *r_rep = (cr_r_rep_big_t *) (((void *) r_rep_mes) + byte_ptr);

    if (DEBUG_READ_REPS)
      my_printf(yellow, "Wrkr: %u R_rep %u/%u opcode %u, session %u, byte_ptr %u\n",
                ctx->t_id, r_rep_i, r_rep_mes->coalesce_num,
                r_rep->opcode, r_rob->sess_id, byte_ptr);
    check_state_with_allowed_flags(3, r_rep->opcode, VERSION_DIFF, VERSION_EQUAL);
    if (ENABLE_ASSERTIONS) {
      assert(r_rob->state == VALID);
      assert(r_rob->l_id == r_rep_mes->l_id + r_rep_i);
    }
    if (r_rep->opcode == VERSION_DIFF) {
      byte_ptr += R_REP_BIG_SIZE;
      memcpy(r_rob->value_to_read, r_rep->value, VALUE_SIZE);
    }
    else byte_ptr += R_REP_SMALL_SIZE;

    cr_ctx->committed_r_id++;
    cr_ctx->stalled[r_rob->sess_id] = false;
    cr_ctx->all_sessions_stalled = false;
    r_rob->state = INVALID;
    fifo_incr_pull_ptr(r_rob_fifo);
    fifo_decrem_capacity(r_rob_fifo);
  }

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
    ctx_send_unicasts(ctx, PREP_QP_ID);

    //
    ctx_poll_incoming_messages(ctx, PREP_QP_ID);

    //
    ctx_send_acks(ctx, ACK_QP_ID);

    //
    ctx_poll_incoming_messages(ctx, ACK_QP_ID);

    if (CR_REMOTE_READS) {
      ctx_send_unicasts(ctx, R_QP_ID);
      ctx_poll_incoming_messages(ctx, R_QP_ID);
    }

    //
    if (ctx->m_id != CR_TAIL_NODE) {
      cr_commit_writes(ctx);

    }

  }
}

#endif //ODYSSEY_CR_INLINE_UTIL_H
