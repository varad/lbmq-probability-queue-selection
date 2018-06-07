/*
 * $Id: Distribution.java 39356 2018-06-07 09:28:15Z radek.varbuchta $
 *
 * Copyright (c) 2018 AspectWorks, spol. s r.o.
 */
package lbmq;

/**
 * Probability distribution - how often should a queue of the certain priority be chosen.
 */
public interface Distribution {

    /**
     * How often should a queue of the certain priority be chosen.
     *
     * @param priority priority
     * @return weight
     */
    long getWeight(int priority);

}
