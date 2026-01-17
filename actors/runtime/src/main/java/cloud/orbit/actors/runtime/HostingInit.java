/*
 Copyright (C) 2019 Electronic Arts Inc.  All rights reserved.

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

package cloud.orbit.actors.runtime;

import cloud.orbit.actors.cluster.NodeAddress;

import java.io.Serializable;
import java.util.Map;

public class HostingInit implements Serializable
{
    private static final long serialVersionUID = 1L;

    private final String nodeName;
    private final NodeAddress nodeAddress;
    private final Map<String, Integer> supportedActorInterfaces;

    public HostingInit(final String nodeName, final NodeAddress nodeAddress,
                       final Map<String, Integer> supportedActorInterfaces)
    {
        this.nodeName = nodeName;
        this.nodeAddress = nodeAddress;
        this.supportedActorInterfaces = supportedActorInterfaces;
    }

    public String getNodeName()
    {
        return nodeName;
    }

    public NodeAddress getNodeAddress()
    {
        return nodeAddress;
    }

    public Map<String, Integer> getSupportedActorInterfaces()
    {
        return supportedActorInterfaces;
    }

    @Override
    public String toString()
    {
        return "HostingInit{" +
                "nodeName=" + nodeName +
                ", nodeAddress=" + nodeAddress +
                ", supportedActorInterfaces=" + supportedActorInterfaces.keySet() +
                '}';
    }
}
