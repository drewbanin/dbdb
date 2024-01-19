

const cleanDag = (data) => {
    const {nodes, edges, query_id} = data;

    // remove scope nodes
    // connect scope child to scope parent

    const scopeNodes = nodes.filter(n => n.name === 'Scope');
    scopeNodes.forEach(node => {
        const parentNodes = edges[node.id] || [];
        const childNodes = Object.keys(edges).filter(n => {
            const nodeList = edges[n] || [];

            const newEdges = nodeList.filter(n2 => {
                return n2 !== node.id
            })

            // we removed our scope node
            if (newEdges.length != nodeList.length) {
                parentNodes.forEach(pn => {
                    newEdges.push(pn)
                })
            }

            edges[n] = newEdges;
        })
    })

    const newNodes = nodes.filter(n => n.name !== 'Scope');

    return {
        nodes: newNodes,
        edges: edges,
        query_id: query_id,
    }
}


export { cleanDag }
