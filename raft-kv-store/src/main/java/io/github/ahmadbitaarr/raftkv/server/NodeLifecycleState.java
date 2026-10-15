package io.github.ahmadbitaarr.raftkv.server;
/** READY is reserved for future phases; Phase 5 never enters it. */
public enum NodeLifecycleState {
    NEW, STARTING, RECOVERING, CONTROL_ONLY, READY, FAILED, STOPPING, STOPPED;
    public boolean canTransitionTo(NodeLifecycleState next){
        if(next==this && (this==STOPPING || this==STOPPED))return true;
        if(next==READY)return false;
        if(next==STOPPING)return this!=STOPPED;
        return switch(this){
            case NEW -> next==STARTING;
            case STARTING -> next==RECOVERING || next==FAILED;
            case RECOVERING -> next==CONTROL_ONLY || next==FAILED;
            case CONTROL_ONLY -> next==FAILED;
            case FAILED -> next==STOPPING;
            case STOPPING -> next==STOPPED || next==FAILED;
            default -> false;
        };
    }
}
