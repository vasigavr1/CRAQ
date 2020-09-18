//
// Created by vasilis on 18/09/20.
//

#ifndef ODYSSEY_CR_GENERIC_UTIL_H
#define ODYSSEY_CR_GENERIC_UTIL_H


#include <cr_config.h>

static inline bool is_head(context_t *ctx)
{
  return  ctx->m_id == CR_HEAD_NODE;
}

static inline bool is_tail(context_t *ctx)
{
  return  ctx->m_id == CR_TAIL_NODE;
}

static inline bool is_middle(context_t *ctx)
{
  return ctx->m_id != CR_TAIL_NODE && ctx->m_id != CR_HEAD_NODE;
}

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


static inline void cr_free_session(context_t * ctx,
                                   uint16_t sess_id)
{

  cr_ctx_t *cr_ctx = (cr_ctx_t *) ctx->appl_ctx;
  if (ENABLE_ASSERTIONS) {
    assert(sess_id < SESSIONS_PER_THREAD);
    if (!cr_ctx->stalled[sess_id]) {
      if (DEBUG_WRITES)
        my_printf(red, "Session not stalled: owner %u/%u \n",
                 ctx->m_id, sess_id);
      assert(false);
    }
    else {
      if (DEBUG_WRITES)
        my_printf(green, "Session is stalled: owner %u/%u \n",
                  ctx->m_id, sess_id);
    }
  }


  cr_ctx->all_sessions_stalled = false;
  signal_completion_to_client(sess_id,
                              cr_ctx->index_to_req_array[sess_id],
                              ctx->t_id);
  cr_ctx->stalled[sess_id] = false;
}



#endif //ODYSSEY_CR_GENERIC_UTIL_H
