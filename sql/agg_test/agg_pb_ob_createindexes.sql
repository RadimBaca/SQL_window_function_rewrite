
I(A):CREATE INDEX idx_tab_a ON tab(a);
I(B):CREATE INDEX idx_tab_b ON tab(b);
I(A);I(B):CREATE INDEX idx_tab_a ON tab(a);CREATE INDEX idx_tab_b ON tab(b);
I(AB):CREATE INDEX idx_tab_a_b ON tab(a,b);
I(BA):CREATE INDEX idx_tab_b_a ON tab(b,a);
I(C):CREATE INDEX idx_tab_c ON tab(c);
I(C);I(A):CREATE INDEX idx_tab_c ON tab(c);CREATE INDEX idx_tab_a ON tab(a);
I(C);I(B):CREATE INDEX idx_tab_c ON tab(c);CREATE INDEX idx_tab_b ON tab(b);
I(C);I(A);I(B):CREATE INDEX idx_tab_c ON tab(c);CREATE INDEX idx_tab_a ON tab(a);CREATE INDEX idx_tab_b ON tab(b);
I(C);I(AB):CREATE INDEX idx_tab_c ON tab(c);CREATE INDEX idx_tab_a_b ON tab(a,b);
I(C);I(BA):CREATE INDEX idx_tab_c ON tab(c);CREATE INDEX idx_tab_b_a ON tab(b,a);