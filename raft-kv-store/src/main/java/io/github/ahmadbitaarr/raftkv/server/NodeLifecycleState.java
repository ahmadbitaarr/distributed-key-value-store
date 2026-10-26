package io.github.ahmadbitaarr.raftkv.server;
/** Controlled leader READY; shutdown and failure remain terminal for read admission. */
public enum NodeLifecycleState {
    NEW, STARTING, RECOVERING, CONTROL_ONLY, READY, FAILED, STOPPING, STOPPED;
    public boolean canTransitionTo(NodeLifecycleState next){
        if(next==this && (this==STOPPING || this==STOPPED))return true;
        if(next==STOPPING)return this!=STOPPED;
        return switch(this){
            case NEW -> next==STARTING;
            case STARTING -> next==RECOVERING || next==FAILED;
            case RECOVERING -> next==CONTROL_ONLY || next==FAILED;
            case CONTROL_ONLY -> next==READY || next==FAILED;
            case READY -> next==FAILED;
            case FAILED -> next==STOPPING;
            case STOPPING -> next==STOPPED || next==FAILED;
            default -> false;
        };
    }
}
