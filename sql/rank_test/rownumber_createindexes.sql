
I(A):CREATE INDEX idx_tab_a ON tab(a);
I(B):CREATE INDEX idx_tab_b ON tab(b);
I(A);I(B):CREATE INDEX idx_tab_a ON tab(a);CREATE INDEX idx_tab_b ON tab(b);
I(AB):CREATE INDEX idx_tab_a_b ON tab(a,b);
I(BA):CREATE INDEX idx_tab_b_a ON tab(b,a);