/*
 Copyright (C) 2016 Electronic Arts Inc.  All rights reserved.

 Redistribution and use in source and binary forms, with or without
 modification, are permitted provided that the following conditions
 are met:

 1.  Redistributions of source code must retain the above copyright
     notice, this list of conditions and the following disclaimer.
 2.  Redistributions in binary form must reproduce the above copyright
     notice, this list of conditions and the following disclaimer in the
     documentation and/or other materials provided with the distribution.
 3.  Neither the name of Electronic Arts, Inc. ("EA") nor the names of
     its contributors may be used to endorse or promote products derived
     from this software without specific prior written permission.

 THIS SOFTWARE IS PROVIDED BY ELECTRONIC ARTS AND ITS CONTRIBUTORS "AS IS" AND ANY
 EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 DISCLAIMED. IN NO EVENT SHALL ELECTRONIC ARTS OR ITS CONTRIBUTORS BE LIABLE FOR ANY
 DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package cloud.orbit.actors.cluster;

import cloud.orbit.actors.runtime.RemoteKey;
import cloud.orbit.concurrent.Task;
import cloud.orbit.exception.UncheckedException;
import cloud.orbit.util.IOUtils;
import cloud.orbit.util.StringUtils;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ClusteringConfigurationBuilder;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.fork.ForkChannel;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.protocols.FRAG2;
import org.jgroups.protocols.FRAG3;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinTask;

public class JGroupsClusterPeer implements ExtendedClusterPeer
{

    private static final Logger logger = LoggerFactory.getLogger(JGroupsClusterPeer.class);
    public static final String JGROUPS_XML_DEFAULT = "classpath:/conf/udp-jgroups.xml";

    private final Executor executor;
    private final boolean zeroCapacityFactor;

    private int portRangeLength = 1000;
    private Task<Address> startFuture;
    private ForkChannel channel;
    private DefaultCacheManager cacheManager;
    private NodeInfo local;

    private NodeInfo master;
    private final Map<Address, NodeInfo> nodeMap = new ConcurrentHashMap<>();
    private final Map<NodeAddress, NodeInfo> nodeMap2 = new ConcurrentHashMap<>();
    private ViewListener viewListener;
    private MessageListener messageListener;

    private String jgroupsConfig = JGROUPS_XML_DEFAULT;

    private boolean nameBasedUpdPort = true;
    private final List<String> additionalCacheNames = new ArrayList<>();

    public JGroupsClusterPeer()
    {
        this(JGROUPS_XML_DEFAULT, Runnable::run, false);
    }

    public JGroupsClusterPeer(final String jgroupsConfig, final Executor executor, final boolean zeroCapacityFactor)
    {
        this.executor = executor;
        this.jgroupsConfig = jgroupsConfig;
        this.zeroCapacityFactor = zeroCapacityFactor;
    }

    @Override
    public NodeAddress localAddress()
    {
        sync();
        return local.nodeAddress;
    }

    @Override
    public void registerViewListener(final ViewListener viewListener)
    {
        this.viewListener = viewListener;
    }

    @Override
    public void registerMessageReceiver(final MessageListener messageListener)
    {
        this.messageListener = messageListener;
    }

    private static final class NodeInfo
    {
        private final Address address;
        private final NodeAddress nodeAddress;

        NodeInfo(final Address address)
        {
            this.address = address;
            final UUID jgroupsUUID = (UUID) address;
            this.nodeAddress = new NodeAddressImpl(new java.util.UUID(jgroupsUUID.getMostSignificantBits(), jgroupsUUID.getLeastSignificantBits()));
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            final NodeInfo nodeInfo = (NodeInfo) o;

            return address.equals(nodeInfo.address);
        }

        @Override
        public int hashCode()
        {
            return address.hashCode();
        }
    }

    @Override
    public Task<?> join(final String clusterName, final String nodeName)
    {
        startFuture = new Task<>();
        final ForkJoinTask<Void> f = ForkJoinTask.adapt(() ->
        {
            final InputStream configInputStream = configToURL(getJgroupsConfig()).openStream();
            try
            {
                if (System.getProperty("java.net.preferIPv4Stack", null) == null)
                {
                    System.setProperty("java.net.preferIPv4Stack", "true");
                }

                final GlobalConfigurationBuilder globalConfigurationBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();

                ClassAllowList classAllowList = new ClassAllowList();
                classAllowList.addClasses(RemoteKey.class, NodeAddress.class, NodeAddressImpl.class, java.util.UUID.class);
                globalConfigurationBuilder.cacheContainer().serialization().marshaller(new JavaSerializationMarshaller(classAllowList));

                globalConfigurationBuilder.transport()
                        .clusterName(clusterName)
                        .nodeName(nodeName)
                        .addProperty(JGroupsTransport.CONFIGURATION_XML, StringUtils.convertInputStreamToString(configInputStream));

                cacheManager = new DefaultCacheManager(globalConfigurationBuilder.build(), true);

                ConfigurationBuilder builder = new ConfigurationBuilder();
                ClusteringConfigurationBuilder configurationBuilder = builder.clustering().cacheMode(CacheMode.DIST_ASYNC);
                configurationBuilder.partitionHandling()
                        .whenSplit(PartitionHandling.DENY_READ_WRITES)
                        .mergePolicy(MergePolicy.PREFERRED_NON_NULL);
                if (zeroCapacityFactor)
                {
                    logger.info("Setting capacity factor to 0.00001f");
                    configurationBuilder.hash().capacityFactor(0.00001f); // see ISPN-4996
                }
                Configuration sharedConfiguration = builder.build();

                // Orbit actor registry
                cacheManager.defineConfiguration("distributedDirectory", sharedConfiguration);

                for (String cacheName : getNormalizedCacheNames())
                {
                    cacheManager.defineConfiguration(cacheName, sharedConfiguration);
                }

                if (cacheManager.getTransport() instanceof JGroupsTransport)
                {
                    JGroupsTransport jGroupsTransport = (JGroupsTransport) cacheManager.getTransport();

                    ProtocolStack stack = jGroupsTransport.getChannel().getProtocolStack();

                    Class<? extends Protocol> neighborProtocol = stack.findProtocol(FRAG2.class) != null ?
                            FRAG2.class : FRAG3.class;
                    channel = new ForkChannel(jGroupsTransport.getChannel(),
                            "hijack-stack",
                            "lead-hijacker",
                            false,
                            ProtocolStack.Position.ABOVE,
                            neighborProtocol);

                    channel.setReceiver(new ReceiverAdapter()
                    {

                        @Override
                        public void viewAccepted(final View view)
                        {
                            doViewAccepted(view);
                        }

                        @Override
                        public void receive(final MessageBatch batch)
                        {
                            Task.runAsync(() -> {
                                for (Message message : batch)
                                {
                                    try
                                    {
                                        doReceive(message);
                                    }
                                    catch (Throwable ex)
                                    {
                                        logger.error("Error receiving batched message", ex);
                                    }
                                }
                            }, executor).exceptionally((e) -> {
                                logger.error("Error receiving message", e);
                                return null;
                            });
                        }

                        @Override
                        public void receive(final Message msg)
                        {
                            Task.runAsync(() -> doReceive(msg), executor).exceptionally((e) -> {
                                logger.error("Error receiving message", e);
                                return null;
                            });
                        }
                    });

                    JmxConfigurator.registerChannel(jGroupsTransport.getChannel(), ManagementFactory.getPlatformMBeanServer(), "org.jgroups", clusterName, true);
                }

                // need to get a cache, any cache to force the initialization
                cacheManager.getCache("distributedDirectory");

                channel.connect(clusterName);
                local = new NodeInfo(channel.getAddress());
                logger.info("Fork-channel running");
                logger.info("Done with JGroups initialization");

                startFuture.complete(local.address);
            }
            catch (final Exception e)
            {
                logger.error("Error during JGroups initialization", e);
                startFuture.completeExceptionally(e);
            }
            finally
            {
                IOUtils.silentlyClose(configInputStream);
            }
            return null;
        });
        f.fork();
        return startFuture;
    }

    private URL configToURL(final String jgroupsConfig) throws MalformedURLException
    {
        if (jgroupsConfig.startsWith("classpath:"))
        {
            // classpath resource
            final String resourcePath = jgroupsConfig.substring("classpath:".length());
            final URL resource = getClass().getResource(resourcePath);
            if (resource == null)
            {
                throw new IllegalArgumentException("Can't find classpath resource: " + resourcePath);
            }
            return resource;
        }
        if (!jgroupsConfig.contains(":"))
        {
            // normal file
            return Paths.get(jgroupsConfig).toUri().toURL();
        }
        return new URL(jgroupsConfig);
    }

    @Override
    public void leave()
    {
        channel.close();
        channel = null;
        cacheManager.stop();
    }

    // ensures that the channel is connected
    private void sync()
    {
        if (startFuture != null && !startFuture.isDone())
        {
            startFuture.join();
        }
    }

    private void doViewAccepted(final View view)
    {
        final ConcurrentHashMap<Address, NodeInfo> newNodes = new ConcurrentHashMap<>(view.size());
        final ConcurrentHashMap<NodeAddress, NodeInfo> newNodes2 = new ConcurrentHashMap<>(view.size());
        for (final Address a : view)
        {
            NodeInfo info = nodeMap.get(a);
            if (info == null)
            {
                info = new NodeInfo(a);
            }
            newNodes.put(a, info);
            newNodes2.put(info.nodeAddress, info);
        }

        final NodeInfo newMaster = newNodes.values().iterator().next();

        nodeMap.putAll(newNodes);
        nodeMap.values().retainAll(newNodes.values());
        nodeMap2.putAll(newNodes2);
        nodeMap2.values().retainAll(newNodes2.values());

        master = newMaster;
        viewListener.onViewChange(nodeMap2.keySet());
    }

    @SuppressWarnings("PMD.AvoidThrowingNullPointerException")
    @Override
    public void sendMessage(final NodeAddress address, final byte[] message)
    {
        final NodeInfo node = nodeMap2.get(Objects.requireNonNull(address, "node address"));
        if (node == null)
        {
            throw new IllegalArgumentException("Cluster node not found: " + address);
        }
        try
        {
            this.channel.send(node.address, message);
        }
        catch (Exception e)
        {
            throw new UncheckedException(e);
        }
    }

    @Override
    public <K, V> DistributedMap<K, V> getCache(final String name)
    {
        return new InfinispanDistributedMap<>(cacheManager.getCache(name));
    }

    @Override
    public <K, V> AdvancedCache<K, V> getAdvancedCache(final String name)
    {
        Cache<K, V> cache = cacheManager.getCache(name);
        return cache.getAdvancedCache();
    }

    private void doReceive(final Message msg)
    {
        final NodeInfo nodeInfo = nodeMap.get(msg.getSrc());
        if (nodeInfo == null)
        {
            logger.warn("Received message from invalid address {}", msg.getSrc());
            messageListener.receive(new NodeAddressImpl(new java.util.UUID(((UUID) msg.getSrc()).getMostSignificantBits(), ((UUID) msg.getSrc()).getLeastSignificantBits())), msg.getBuffer());
        }
        else
        {
            messageListener.receive(nodeInfo.nodeAddress, msg.getBuffer());
        }
    }

    public NodeAddress getMaster()
    {
        return master != null ? master.nodeAddress : null;
    }

    public String getJgroupsConfig()
    {
        return jgroupsConfig;
    }

    public void setJgroupsConfig(final String jgroupsConfig)
    {
        this.jgroupsConfig = jgroupsConfig;
    }

    public boolean isNameBasedUpdPort()
    {
        return nameBasedUpdPort;
    }

    public void setNameBasedUpdPort(final boolean nameBasedUpdPort)
    {
        this.nameBasedUpdPort = nameBasedUpdPort;
    }

    public int getPortRangeLength()
    {
        return portRangeLength;
    }

    public void setPortRangeLength(final int portRangeLength)
    {
        this.portRangeLength = portRangeLength;
    }

    public void setCacheNames(final List<String> cacheNames)
    {
        additionalCacheNames.clear();
        if (cacheNames != null)
        {
            additionalCacheNames.addAll(cacheNames);
        }
    }

    public void addCacheName(final String cacheName)
    {
        Objects.requireNonNull(cacheName, "cacheName");
        if (cacheName.isEmpty())
        {
            throw new IllegalArgumentException("cacheName must not be empty");
        }
        additionalCacheNames.add(cacheName);
    }

    public List<String> getCacheNames()
    {
        return Collections.unmodifiableList(additionalCacheNames);
    }

    private List<String> getNormalizedCacheNames()
    {
        if (additionalCacheNames.isEmpty())
        {
            return Collections.emptyList();
        }
        final Set<String> cacheNames = new HashSet<>();
        for (String cacheName : additionalCacheNames)
        {
            if (cacheName == null || cacheName.isEmpty())
            {
                continue;
            }
            if ("distributedDirectory".equals(cacheName))
            {
                continue;
            }
            cacheNames.add(cacheName);
        }
        if (cacheNames.isEmpty())
        {
            return Collections.emptyList();
        }
        return new ArrayList<>(cacheNames);
    }
}
