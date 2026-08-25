import './styles.css';
import { mount } from 'svelte';
import Sql from './pages/Sql.svelte';

mount(Sql, { target: document.getElementById('app') as HTMLElement });
