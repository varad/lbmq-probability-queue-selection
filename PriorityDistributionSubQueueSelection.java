/*
 * $Id: PriorityDistributionSubQueueSelection.java 39356 2018-06-07 09:28:15Z radek.varbuchta $
 *
 * Copyright (c) 2018 AspectWorks, spol. s r.o.
 */
package lbmq;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Implementations of {@link lbmq.LinkedBlockingMultiQueue.SubQueueSelection} which chooses items from higher priority
 * queues more often than items from lower priority queues. How often they are chosen is based on probability.
 *
 * Unlike {@link LinkedBlockingMultiQueue}, we have just 5 priorities (highest, high, normal, low, lowest).
 *
 * We define a "probability distribution": each priority has a ratio assigned which defines how often it
 * should be chosen. For example: Lowest = 1, Low = 2, Normal = 4, High = 8, Highest = 16
 *
 * When choosing the next queue we:
 * 1) Create an array of priorities. Each priority has N cells. (N = ratio of the given priority) * number of queues
 *    of that priority
 * 2) We randomly choose a priority from the array
 * 3) We select a queue of that priority using round-robin. If all those queues are empty we fallback to the default
 *    implementation ({@link DefaultSubQueueSelection} and choose a queue based on priority.
 *
 * Example: - we have 2 active Low priority queues and 1 active High priority queue.
 *          - probability distribution: Lowest = 1, Low = 2, Normal = 4, High = 8, Highest = 16
 * 1) We build an array: |LOW|LOW|LOW|LOW|HIGH|HIGH|HIGH|HIGH|HIGH|HIGH|HIGH|HIGH|
 * 2) We randomly choose one of the cells.
 * 3) We select a queue of the chosen priority using round robin.
 */
public class PriorityDistributionSubQueueSelection<K, E> implements LinkedBlockingMultiQueue.SubQueueSelection<K, E> {

    /**
     * Priority groups from {@link LinkedBlockingMultiQueue}. It's the same instance as the one in
     * {@link LinkedBlockingMultiQueue}!
     */
    private ArrayList<LinkedBlockingMultiQueue<K, E>.PriorityGroup> priorityGroups;

    /**
     * Random number generator.
     */
    private lbmq.PriorityDistributionSubQueueSelection.RandomNumberProvider randomNumberGenerator = new lbmq.PriorityDistributionSubQueueSelection.JavaRandomBasedRandomNumberProvider();

    /**
     * Index of the next priority group in {@code priorityGroups} which should be used to retrieve
     * the next subQueue. We have to pre-generate and remember the value so that "peek" method
     * can return the same value repeatedly (in case we did not add or remove values from any queue).
     */
    private int nextPriorityGroupIdx = -1;

    /**
     * Probability distribution.
     */
    private Distribution distribution;

    /**
     * Default implementation of {@link lbmq.LinkedBlockingMultiQueue.SubQueueSelection}. Used in case
     * we have chosen a priority group whose queues are currently empty and therefore we have to choose
     * a different priority group.
     */
    private DefaultSubQueueSelection defaultSubQueueSelection = new DefaultSubQueueSelection();

    @Override
    public LinkedBlockingMultiQueue.SubQueue getNext() {
        regenerateNextPriorityGroupIdxIfNecessary();

        LinkedBlockingMultiQueue<K, E>.SubQueue subQueue = priorityGroups.get(nextPriorityGroupIdx).getNextSubQueue();

        // the selected priority queue is empty => choose a different one by priority
        if (subQueue == null) {
            return defaultSubQueueSelection.getNext();
        }

        regenerateNextPriorityGroupIdx();

        return subQueue;
    }

    private void regenerateNextPriorityGroupIdx() {
        long weightSum = weightSum();
        long randomNumber = randomNumberGenerator.nextLong(weightSum);

        long weightConsumed = 0;
        int i = 0;
        while (i < priorityGroups.size() && (weightConsumed - 1) < randomNumber) {
            int priority = priorityGroups.get(i).priority;
            weightConsumed += distribution.getWeight(priority);
            nextPriorityGroupIdx = i;
            i++;
        }
    }

    private void regenerateNextPriorityGroupIdxIfNecessary() {
        // a) -1: have to regenerate because the index has not been generated yet
        // b) nextPriorityGroupIdx >= priorityGroups.size(): if priorityGroups changed
        // in a way that the index is out of bounds we recover by using the default
        // selection strategy (which prefers queues with the highest priority)
        if (nextPriorityGroupIdx == -1 || nextPriorityGroupIdx >= priorityGroups.size()) {
            regenerateNextPriorityGroupIdx();
        }
    }

    private long weightSum() {
        long weightSum = 0;
        for (int i = 0; i < priorityGroups.size(); i++) {
            int priority = priorityGroups.get(i).priority;
            weightSum += distribution.getWeight(priority);
        }
        return weightSum;
    }

    @Override
    public E peek() {
        // assert takeLock.isHeldByCurrentThread();
        regenerateNextPriorityGroupIdxIfNecessary();
        return priorityGroups.get(nextPriorityGroupIdx).peek();
    }

    @Override
    public void setPriorityGroups(ArrayList<LinkedBlockingMultiQueue<K, E>.PriorityGroup> priorityGroups) {
        this.priorityGroups = priorityGroups;
        defaultSubQueueSelection.setPriorityGroups(priorityGroups);
    }

    public void setRandomNumberGenerator(PriorityDistributionSubQueueSelection.RandomNumberProvider randomNumberGenerator) {
        this.randomNumberGenerator = randomNumberGenerator;
    }

    public void setDistribution(Distribution distribution) {
        this.distribution = distribution;
    }

    /**
     * Generates random numbers. It allows us to choose a different random number generator.
     * That's especially useful for testing where we can provide static "random" number generator.
     */
    public interface RandomNumberProvider {

        /**
         * Generates a random number from <0, max).
         *
         * @param max max number (exclusive)
         * @return a random number
         */
        long nextLong(long max);

    }

    /**
     * An implementation of {@link lbmq.PriorityDistributionSubQueueSelection.RandomNumberProvider} which generates random numbers
     * using Java's implementation.
     */
    public static class JavaRandomBasedRandomNumberProvider implements lbmq.PriorityDistributionSubQueueSelection.RandomNumberProvider {

        @Override
        public long nextLong(long max) {
            return ThreadLocalRandom.current().nextLong(max);
        }

    }

}
